#!/usr/bin/python


# see: rex7


import httplib
import urllib
import xml.dom.minidom

import pymssql
import MySQLdb
import time
import cgi
import re
import sys
import logging
import logging.config
import os.path
import socket

from datetime import datetime
from pytz import timezone
import pytz


# see mdw_items error_status
#  -3   folder id not found
#  -4   previously delivered
#  -5   bad radio (offset supplied)
#  -6   timeout fetching keyframe

socket.setdefaulttimeout(8)   # timeout for all socket requests
		

#############################################

NAS_DIR = '/mnt/nas_images'   # where you're storing keyframe image files

#WSDL_HOST = 'mdwapp1.na1.ad.group'	# wsdl location (production)
WSDL_HOST = 'mdwapp1-int.na1.ad.group'   # wsdl location (integration)
WSDL_URL  = '/MDW_Services/ws/mdw.wsdl'
APPNAME   = 'pinpoint' # <<< DEBUG
SOURCE_ID  = 6  # <<< Source System ID for Pinpoint (same for production and integration)
ITEMS_PER_SOAP_ENVEL  = 10  # <<< Max number of items per SOAP envelope
# GETHITS_LIMIT = 600  # <<< Limit on "gethits" select statement HARDCODED BELOW

CM_URL_DEBUG = 0  # set to 1 if you need to generate staging Critical Mention URLs, set to 0 for PRD

# Pinpoint db settings

MYSQL_PP_HOST = 'queen1.multivisioninc.com'
MYSQL_PP_USER = 'root'
MYSQL_PP_PWD  = ''
MYSQL_PP_DB   = 'uploads'


# cy3

SSQL_CY3_HOST	= 'odsdb1.qwestcolo.local'
SSQL_CY3_USER = 'bnclogin'
SSQL_CY3_PWD  = 'dbl0g1n'
SSQL_CY3_DB   = 'cy3'

# RDS API

RDSAPI_HOST   = 'rdssvc.na1.ad.group'
RDSAPI_PORT   = 82


# check this conf file for logging settings
logging.config.fileConfig('/usr/local/Production/python/ppMdw.conf')
logger = logging.getLogger('cision.ppMdw')

pp_to_listid  = {}  # map between Pinpoint inbox IDs and CY3 List IDs

##mdwent = pymssql.connect(host='mdwsqlclus.na1.ad.group',database='MDW_Entourage',user='mdw_entourage_user',password='WXOO9p1234') 
mdwent = pymssql.connect(host='mdwdb1-int.na1.ad.group',database='MDW_Entourage',user='mdw_entourage_user',password='WXOO9p1234') 
kfqueue = {}


#############################################

# initialize data structures needed for processing.
# - get the Pinpoint inbox <-> List ID data
def _init():
	logger.debug("_init()")
	conn = pymssql.connect(host=SSQL_CY3_HOST,database=SSQL_CY3_DB,user=SSQL_CY3_USER,password=SSQL_CY3_PWD) 
	cur = conn.cursor()
	cur.execute('SELECT ExternalListID,ListID FROM ExternalClientList WITH (NOLOCK)')
	row = cur.fetchone()
	while row:
		pp_to_listid[row[0].lower()] = row[1]
		row = cur.fetchone()
	conn.close()



# used in gathering the text nodes from a parsed XML document
def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)


# translated from Showroom code
def _extract (tstr):
	if len(tstr) <= 515:
		return tstr

	result = tstr[0:515]
	lastIndex = len(result) - 1

	rex = re.compile(r' \S+$')
	sb = rex.sub(' ',result,1)
	if result.find('<b>') >= 0 and result.find('</b>') == -1:
		sb += '</b>'

	sb += '</p>'

	return sb


# parse out start time, duration, and fulltext from XML-encoded text
def _massageFulltext ( ppid, txt, opt_crt):
	result_str = ""
	offset = "0"
	duration = "0"
	txt_timestamp = ""
	
	if txt == None:
		return ("0","0","", "")

# Calculating the clip start time from the crt value isn't working. This code
# grabs the time stamp from the fulltext.
	timerex = re.compile(r'<p>(\d\d:\d\d:\d\d)\.\d\d.+')
	m = timerex.match(txt)
	if (m != None):
		txt_timestamp = m.group(1)


	if txt.find('<summary time') >= 0:
		doc = xml.dom.minidom.parseString(txt)
		itemlist = doc.getElementsByTagName('summary')
		if itemlist[0].getAttribute('crt') != "":
			foo = re.compile(r'M(\d+)\.(\d+)')
			m = foo.match(itemlist[0].getAttribute('crt'))
			offset   = m.group(1)
			duration = m.group(2)

		result_str = ""
		initial_timestamp = ""
		if itemlist[0].getAttribute('time') != "":
			initial_timestamp = itemlist[0].getAttribute('time')

		for i in itemlist:
			this_str = i.childNodes[0].nodeValue
			if initial_timestamp != "":
				this_str = initial_timestamp + " " + this_str
				initial_timestamp = ""
			
			if result_str != "":
				result_str += " "
			
			result_str += "<p> " + this_str  + " </p>"
	elif opt_crt != "":
      # 40.119" ClipLen= 121022
      # M128.115  121022

		if ( opt_crt[0] =='-'):
			opt_crt = opt_crt[ 1:]

		if ( opt_crt.find( 'ClipLen') > 0):
			opt_crt.replace( '" ClipLen=', '')
			opt_crt = 'M'+opt_crt
		result_str = txt
		foo = re.compile(r'[LM](\d+)\.(\d+)')
		m = foo.match(opt_crt)
		if ( m != None):        # 121010 BW
			#logger.debug("UNEXPECTED %s seeking opt_crt %s -------- %s" % ( ppid, opt_crt,txt[0:100]))
			offset   = m.group(1)
			duration = m.group(2)
		else:
			logger.debug("UNEXPECTED %s opt_crt did not parse %s -------- %s" % ( ppid, opt_crt,txt[0:100]))

	else:
		result_str = txt
		logger.debug("UNEXPECTED %s opt_crt empty -------- %s" % ( ppid, txt[0:100]))

	result_str = result_str.replace('<span class="hit-highlight">','<b>')
	result_str = result_str.replace('</span>','</b>')
	return (offset,duration,result_str,txt_timestamp)



# parse important messages from the soap response 121016
def _soapReturnParse( soap_resp, soap_xml):
	global existing
	global bad_radio
	global bad_media_ids
	global bad_keyframes
	global hits_redundant, hits_bad_media_id, hits_bad_soap_envelope


# ERROR Exception trying to connect to WSDL on mdwapp1-int.na1.ad.group: None

	# parsing out a few exception messages that can be auto-corrected by this script
	
   #Item with OriginItemId 466121002151030043 and SourceSystem Pinpoint already exists

	rex1 = re.compile(r".+Item with OriginItemId (\w+) and SourceSystem Pinpoint already exists.+", re.IGNORECASE)
	m1 = rex1.match(soap_resp)
	
	rex2 = re.compile(r".+Missing feed type for Media ID (\d+).+", re.IGNORECASE)
	m2 = rex2.match(soap_resp)

	rex3 = re.compile(r".+Invalid news item \(OriginItemId=(\d+)\): Clip Start Offset cannot be present for Radio.+", re.IGNORECASE)
	m3 = rex3.match(soap_resp)

	rex4 = re.compile(r".+Unable to create envelope from given source.+", re.IGNORECASE)
	m4 = rex4.match(soap_resp)

   #The value '' of element 'war:OriginItemId' is not valid
   #The value '' of element 'war:MediaId' is not valid
	rex5 = re.compile(r".+The value '' of element 'war:MediaId' is not valid.+", re.IGNORECASE)
	m5 = rex5.match(soap_resp)

	rex6 = re.compile(r".+The value '' of element 'war:OriginItemId' is not valid.+", re.IGNORECASE)
	m6 = rex6.match(soap_resp)


	if m1:
		#logger.warn("PPID %s already exists in MDW. Will update db." % (m1.group(1)))
		existing.append(m1.group(1))
		hits_redundant = hits_redundant + 1
	elif m2:
		logger.warn("Media ID %s has missing feedtype in RDS API. Will take items with that ID out of queue." % (m2.group(1)))
		bad_media_ids.append(m2.group(1))
		hits_bad_media_id = hits_bad_media_id + 1
	elif m3:
		if ( m3 != None):
			logger.warn("Offset has been supplied for Radio item PPID %s." % (m3.group(1)))
			bad_radio.append(m3.group(1))
		else:
			logger.warn("Offset has been supplied for Radio item PPID unk.")
			bad_radio.append("unknown")
		#return " "
		#bad_radio.append("PPID unknown")
	elif m4:
		logger.warn("Bad SOAP envelope ---- %s" % (soap_xml))
		hits_bad_soap_envelope = hits_bad_soap_envelope + 1
	elif m5:
		logger.warn("Null media ID ------ %s" % ( soap_resp))
		#if ( m5 != None):
			#bad_media_ids.append( m5.group(1))
		#else:
			#bad_media_ids.append( "bad")
	elif m6:
		logger.warn("Bad origin item ID ---- %s" % (soap_resp))
	else:
		logger.warn("Exception, uncategorized - on %s: %s :::: %s" % (WSDL_HOST, sys.exc_info()[1], soap_resp))
		# sys.exit()

	return " "


# SOAP request
def _SOAP_POST(SOAPAction,xml):

	try:
		h = httplib.HTTPConnection(WSDL_HOST)
		headers = {
			'Host' : WSDL_HOST,
			'Content-Type' : 'text/xml; charset=utf-8',
			'Content-Length' : len(xml),
			'SOAPAction' : '"%s"' % SOAPAction,
		}
		h.request ('POST', WSDL_URL, body = xml, headers = headers)
		r = h.getresponse()
		d = r.read()
		#if r.status == 500:
			#logger.debug( "POSTERR %s %s %s" % (r.status, r.reason, d));
		if r.status != 200 and r.status != 500:     # Bruce experiment
			raise ValueError('Error connecting: %s %s: %s' % (r.status, r.reason, d))
		return d
	except:
		errmsg = "%s" % (sys.exc_info()[1],)
		logger.debug("ERRMSG : %s" % (errmsg,))
		_soapReturnParse( errmsg, xml);
		return " "


def _timezone(tztxt):
	
	badformat = { # timezone values that do not conform to the format 'America/city-name'
		'EAS': 'E',
		'EASTERN': 'E',
		'CEN': 'C',
		'CENTRAL': 'C',
		'PAC': 'P',
		'PACIFIC': 'P',
		'ATL': 'A',
		'ATLANTIC': 'A',
		'MOU': 'M',
		'MOUNTAIN': 'M',
		'NEWFOUNDLAND': 'N'
	}
	if (tztxt != 'N/A' and tztxt.find('/') >= 0):
		foo = timezone(tztxt)
		bar = foo.localize(datetime.now())
		return bar.strftime('%Z')

	elif (tztxt in badformat): # weird format
		retval =  badformat[tztxt]
		if (time.localtime().tm_isdst):
			retval += 'DT'
		else:
			retval += 'ST'
		return retval

	else: # default time zone
		if (time.localtime().tm_isdst):
			return 'CDT'
		else:
			return 'CST'


# the CP inbox IDs are the MDW List IDs. Taking this step before reading data allows us to set the ListID appropriately for items
def _preprocess_pp():
	logger.debug("_preprocess_pp()")
	conn = MySQLdb.connect(host = MYSQL_PP_HOST, user = MYSQL_PP_USER, passwd = MYSQL_PP_PWD, db = MYSQL_PP_DB)
	conn.query ("SELECT DISTINCT(hit_folder_id) FROM mdw_item WHERE cisionpoint_inbox_id IS NULL")
	to_update = []
	r = conn.use_result()
	thisrow = r.fetch_row()
	while len(thisrow) > 0:
		to_update.append(thisrow[0][0])
		thisrow = r.fetch_row()

	cur = conn.cursor()
	for i in to_update:
		if pp_to_listid.has_key(i.lower()):
			logger.debug("UPDATE mdw_item SET cisionpoint_inbox_id = %s WHERE cisionpoint_inbox_id IS NULL AND hit_folder_id = '%s'" % (pp_to_listid[i.lower()], i))
			cur.execute("UPDATE mdw_item SET cisionpoint_inbox_id = %s WHERE cisionpoint_inbox_id IS NULL AND hit_folder_id = '%s'" % (pp_to_listid[i.lower()], i))
		else:
			logger.warn("Pinpoint folder id %s not found in cy3" % (i,))

         # working here 121010
			error_msg_for_db = "folder id %s not found" % (i,);
			#cur.execute("UPDATE mdw_item SET error_text = '%s' WHERE cisionpoint_inbox_id IS NULL AND hit_folder_id = '%s'" % (error_msg_for_db, i))
			#cur.execute("UPDATE mdw_item SET error_status = -3 WHERE cisionpoint_inbox_id IS NULL AND hit_folder_id = '%s'" % (i))
			cur.execute("UPDATE mdw_item SET error_text = '%s', error_status = -3 WHERE cisionpoint_inbox_id IS NULL AND hit_folder_id = '%s'" % (error_msg_for_db, i))

# connect to PP database and get list of hits
def _get_hits():
	global hit_number
	conn = MySQLdb.connect(host = MYSQL_PP_HOST, user = MYSQL_PP_USER, passwd = MYSQL_PP_PWD, db = MYSQL_PP_DB)

	logger.debug("_get_hits()")
	rows_returned = 0
	conn.query ("SELECT m.pinpoint_unique_id,hit_folder_id,cisionpoint_inbox_id,media_id,extendedtext,STR_TO_DATE(air_date,'%m/%d/%Y %H:%i'),crt,video_url,keyframe_path,program,station,market,audience,hut,m.hits_for_mdw_id,publicity_value,\
   IF(STR_TO_DATE(air_date,'%m/%d/%Y %H:%i') < date_sub(NOW(), INTERVAL 28 DAY),1,0) `expired`,device_type,cmsessionid,cmtimezone FROM hits_for_mdw m,mdw_item i where m.hits_for_mdw_id=i.hits_for_mdw_id and mdw_id IS NULL and error_status IS NULL and cisionpoint_inbox_id IS NOT NULL LIMIT 600")
	r = conn.use_result()
	thisrow = r.fetch_row()
	return_list = []

	#if ( len(thisrow) > 0 and len(thisrow) < 16):
		#logger.warn("BADROW %s" % ( ''.join(thisrow))  )
		#logger.warn("BADROW %d LEN %d %s" % ( hit_number, len(thisrow), thisrow, )  )

	while len(thisrow) > 0:   # thisrow[0][5]
		#logger.info("   ROW %d len %d get_hits: %s" % (hit_number, len(thisrow), thisrow,))
		if ( len(thisrow[0]) ==20):

			#logger.info("        %d get_hits len: %d" % (hit_number, len(thisrow[0]),))
			#  len( thisrow[0]) should be 19 as of 10/26
			rows_returned = rows_returned + 1
			fulltextTuple = _massageFulltext(thisrow[0][0],thisrow[0][4],thisrow[0][6])  # XML version of fulltext includes start & duration

			this_dict = { 'ppid':thisrow[0][0], 
			'mediaid':thisrow[0][3],
			'clipstart':str(int(fulltextTuple[0])*1000),
			'clipstop':str((int(fulltextTuple[0])+int(fulltextTuple[1])+90)*1000), # calculation taken from Showroom java code
			'duration':str(int(fulltextTuple[1])*1000),
			'durationsecs':str(int(fulltextTuple[1])),
			'fulltext':fulltextTuple[2], 
			'airdate':thisrow[0][5], 
			'airtime':thisrow[0][6], 
			'previewurl':thisrow[0][7], 
			'keyframe':thisrow[0][8], 
			'showname':thisrow[0][9], 
			'stationname':thisrow[0][10], 
			'marketname':thisrow[0][11],
			'audience':thisrow[0][12], 
			'hut':thisrow[0][13],
			'srid':thisrow[0][14],
			'pubval':thisrow[0][15],
			'expired':thisrow[0][16],
			'device_type':thisrow[0][17],
			'cmsessionid':thisrow[0][18],
			'crt':thisrow[0][6],
			'json_timestamp':fulltextTuple[3],
			'json_timezone': _timezone(thisrow[0][19])
			}
		
			if thisrow[0][2] > 0:
				this_dict['listid'] = thisrow[0][2]
				return_list += [ this_dict ]
			elif pp_to_listid.has_key(thisrow[0][1].lower()):
				this_dict['listid'] = pp_to_listid[thisrow[0][1].lower()]
				return_list += [ this_dict ]
			else:
				logger.warn("Hit ID %s has no mapped List ID" % (thisrow[0][0],))
		else:
			logger.warn("SHORTROW %d get_hits len: %d TEXT %s" % (hit_number, len(thisrow[0]), thisrow))

		thisrow = r.fetch_row()

	logger.info("         %d rows returned" % ( rows_returned, ))
	return return_list


# checks media ID via RDS API to see if it's radio
def _isRadio(mediaid):
	hconn=httplib.HTTPConnection(RDSAPI_HOST,RDSAPI_PORT)
	hconn.request('GET','/rdsapirestful/Media/%s' % (mediaid))
	resp = hconn.getresponse()
	data = resp.read()
	if data.find('"FeedType":"Radio"') >= 0:
		return True
	else:
		return False

def _keyframepath (hitid):
	return '/KeyFrames/K1_%s.jpg' % (hitid,)


# add keyframe to fix-keyframes queue (service runs on il-chi-monitor; datbaase is in MDW_Entourage)
def _queueKeyframe (newsitemid, kfurl, hitid):
	sql = "IF (SELECT COUNT(*) FROM KeyframeDownload WITH (NOLOCK) WHERE NewsItemID = %s) = 0 INSERT INTO KeyframeDownload (NewsItemID,KeyframePath, SourceCreationDate, KeyframeURL, [status]) VALUES (%s,'%s',GETDATE(),'%s',2)" % (newsitemid, newsitemid, _keyframepath(hitid), kfurl)
	logger.debug(sql)
	try:
		thiscur = mdwent.cursor()
		thiscur.execute(sql)
		mdwent.commit()
		thiscur.close()
	except:
		logger.warn("could not execute SQL on MDW_Entourage: %s" % (sql,) )
	

# save keyframe to the nas
def _saveKeyframe (kfurl, hitid):
	logger.debug("ADDING key %s, value %s" % (hitid, kfurl))
	kfqueue[hitid] = kfurl
	return _keyframepath (hitid)


#	if kfurl.find('.jpg') >= 0:
#		try:
#			logger.debug("downloading keyframe from Critical Mention")
#			kfimage = urllib.urlopen(kfurl)
#			kfdata = kfimage.read()
#			logger.debug("done downloading keyframe")
#			if kfdata.find('404') > 0 or kfdata.find('<html>') > 0:  # basic check for 404 error
#				return '/KeyFrames/virage.jpg'
#			else:
#				kfuri = _keyframepath (hitid)
#				logger.debug("attempting to write keyframe to NAS")
#				kfout = open(NAS_DIR + kfuri, mode='w')
#				try:
#					kfout.write(kfdata)
#				finally:
#					kfout.close()
#				logger.debug("done with writing keyframe")
#			return kfuri
#		except:
#			#2012-10-23 14:01:54,041 WARNING timeout download keyframe http://cnc119.criticalmention.com/8033/20121009130000/20121009135200.jpg
#			logger.warn("timeout download keyframe %s" % (kfurl,))
#			return "/KeyFrames/virage.jpg"

def _mark_hit_error( ppid, error_no):
	conn = MySQLdb.connect(host = MYSQL_PP_HOST, user = MYSQL_PP_USER, passwd = MYSQL_PP_PWD, db = MYSQL_PP_DB)
	sql = "UPDATE mdw_item SET error_status = %d WHERE pinpoint_unique_id = '%s'" % (error_no, ppid)
	logger.debug(sql)
	conn.query(sql)


# build up our SOAP request
def _build_xml(itemlist):
	global hit_number
	global ppid_processed
	global bad_keyframes

	xml = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:war="http://www.cision.com/monitoring/warehouse">'
	xml += '<soapenv:Header/>'
	xml += '<soapenv:Body>'
	xml += '<war:InsertItemsRequest>'
	xml += '<war:RequestingApplication>'+APPNAME+'</war:RequestingApplication>'
	xml += '<war:BroadcastItems>'
	ppids_in_xml = ''
	
	for i in itemlist:
		hit_number += 1;
		logger.debug("%d Building XML for: Pinpoint ID = %s, hit id = %s" % (hit_number,i['ppid'],i['srid']))
		if i['ppid'] in ppid_processed:
			ppids_in_xml += '\n       %d SKIPPED %s srid %s because already processed' % (hit_number, i['ppid'], i['srid']) 
			#logger.debug("%d        skipping   Pinpoint ID = %s srid %s because already processed" % (hit_number,i['ppid'], i['srid']))
			continue
		if i['airdate'] == None:
			ppids_in_xml += '\n       %d SKIPPED %s because airdate is null' % (hit_number, i['ppid'])
			#logger.debug("%d        skipping   Pinpoint ID = %s because airdate is null" % (hit_number,i['ppid']))
			continue
		ppids_in_xml += '\n       %d into xml %s srid %s' % (hit_number, i['ppid'], i['srid'])

		ppid_processed[ i['ppid']]  = 1


#  121128
#  

# program="Today on 5 (1/2) ^KEstimated"

		keyframe = "/KeyFrames/virage.jpg"
		
		logger.debug("%d          before isRadio." % (hit_number))
		if _isRadio(i['mediaid']):
			sourcetype = 'Radio'
			logger.debug("%d          after  isRadio." % (hit_number))
		elif i['keyframe'] != "" and i['previewurl'] != '' and not i['expired']:
			logger.debug("%d          after isRadio bef saveKeyframe." % (hit_number))
			translated_keyframe = _saveKeyframe(i['keyframe'],i['ppid'])  # unique id could also be hits_for_mdw_id
			if ( translated_keyframe.find( 'virage.jpg') > -1):
				bad_keyframes.append(i['keyframe'])      #   111023
				_mark_hit_error(i['ppid'], -6)        #   111023
				continue
			else:
				keyframe = translated_keyframe
			logger.debug("%d          after saveKeyframe." % (hit_number))
			sourcetype = 'TvWithVideo'
		else:
			sourcetype = 'TvNoVideo'

		dataprovider = ""  # CP needs to know if this clip is coming from CM when it displays the video
		if i['device_type'] == "CM":
			dataprovider = 'Critical Mention'

#		json string required for December 2012 release 
		json = '{"Vnd_Brcast_TimeZone": "'+i['json_timezone']+'", "Vnd_Brcast_AirDate": "' + i['airdate'].strftime("%m/%d/%Y")+'", "Vnd_Brcast_StartTime": "' + i['json_timestamp']+'", "Vnd_Brcast_TotalRunTime": '+ i['durationsecs'] + '}'

		xml += '<war:BroadcastItem IsDeleted="false" IsPES="false">'
		xml += '<war:OriginItemId>%s</war:OriginItemId>' % (i['ppid'],) 
		xml += '<war:ListId>%s</war:ListId>' % (i['listid'],)  
		xml += '<war:MediaId>%s</war:MediaId>' % (i['mediaid'],) 
		xml += '<war:Extract>%s</war:Extract>' % (cgi.escape(_extract(i['fulltext'])),) 
		xml += '<war:FullText> '+cgi.escape(i['fulltext'])+' </war:FullText>'  
		xml += '<war:AirDate>'
		xml += '<war:datePart>'+i['airdate'].strftime("%Y-%m-%d")+'</war:datePart>' 
		xml += '<war:timePart>'+i['airdate'].strftime("%H:%M:%S")+'</war:timePart>' 
		xml += '</war:AirDate>'
		xml += '<war:SourceSystem>Pinpoint</war:SourceSystem>' 
		xml += '<war:SourceCreatedDate>'+time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())+'</war:SourceCreatedDate>'
		xml += '<war:SourceType>'+sourcetype+'</war:SourceType>' 
	
		if sourcetype != "TvNoVideo":

			vurl = i['previewurl']
			if CM_URL_DEBUG == 1:
				vurl = vurl + "&amp;partnerToken=" + i['cmsessionid']
			else:
				vurl = vurl.replace("staging.criticalmention.com","cision.criticalmention.com")
				vurl = vurl + "&amp;partnerToken=" + i['cmsessionid']

			xml += '<war:ClipStartOffset>'+i['clipstart']+'</war:ClipStartOffset>' 
			xml += '<war:ClipStopOffset>'+i['clipstop']+'</war:ClipStopOffset>'
			xml += '<war:ClipDuration>'+i['duration']+'</war:ClipDuration>'
			xml += '<war:VideoUrl>'+vurl+'</war:VideoUrl>'
			xml += '<war:KeyFrameFilePath>'+keyframe+'</war:KeyFrameFilePath>'  ## /KeyFrames/S(1|2)_(hitid).jpg

		xml += '<war:OriginItemIdDomain>090746</war:OriginItemIdDomain>'
	#	xml += '<war:TimecodedText>?</war:TimecodedText>' 
	#	xml += '<war:CopyrightProtection>?</war:CopyrightProtection>'
		xml += '<war:ShowName>'+i['showname']+'</war:ShowName>' 
		xml += '<war:StationName>'+i['stationname']+'</war:StationName>' 
		xml += '<war:MarketName>'+i['marketname']+'</war:MarketName>' 
	#	xml += '<war:AirTimeNotes>?</war:AirTimeNotes>' 
	#	xml += '<war:DmaRank>?</war:DmaRank>'
	#	xml += '<war:DmaId>?</war:DmaId>'
	#	xml += '<war:MsaRank>?</war:MsaRank>'
	#	xml += '<war:MsaId>?</war:MsaId>'
	# xml += '<war:PublicityValue currency="?">?</war:PublicityValue>' 
	#	xml += '<war:TextExpiration>?</war:TextExpiration>'
	#	xml += '<war:AdRate currency="?">?</war:AdRate>'
	#	xml += '<war:AdRateUSD currency="?">?</war:AdRateUSD>'
	#	xml += '<war:ArbitronAverage>?</war:ArbitronAverage>'
		if i['audience'] > 0:
			xml += '<war:Audience>'+str(i['audience'])+'</war:Audience>'
	#	if i['hut'] > 0:
	#		xml += '<war:Hut>'+str(i['hut'])+'</war:Hut>'  # <<< Pinpoint HUT values have been out of acceptable range in dev testing
	#	xml += '<war:Rating>?</war:Rating>'
	#	xml += '<war:Share>?</war:Share>'	 
	#	xml += '<war:Wattage>?</war:Wattage>'
	#	xml += '<war:CustomReach>?</war:CustomReach>'
		xml += '<war:ShowroomData>'
		xml += '<war:ShowroomId>%d</war:ShowroomId>' % (i['srid'],)
		xml += '<war:ShowroomDiagnostics />'
		xml += '</war:ShowroomData>'
	#	xml += '<war:VideoDownloadUrl>?</war:VideoDownloadUrl>'
		if i['pubval'] > 0:
			xml += '<war:PublicityValueUSD currency="USD">'+str(i['pubval'])+'</war:PublicityValueUSD>'

	#	xml += '<war:EstimatedAdValue currency="?">?</war:EstimatedAdValue>'
	#	xml += '<war:EstimatedAdValueUSD currency="?">?</war:EstimatedAdValueUSD>'
	#	xml += '<war:CopyrightOrganization>?</war:CopyrightOrganization>'
	#	xml += '<war:CopyrightOrganizationId>?</war:CopyrightOrganizationId>'
	#	xml += '<war:Sentiment>?</war:Sentiment>'
		xml += '<war:DataProvider>'+dataprovider+'</war:DataProvider>'  ## needs to be populated for Critical Mention clips
	# xml += '<war:NewsItemExpiration>?</war:NewsItemExpiration>'
	# xml += '<war:DigitalExpiration>?</war:DigitalExpiration>'
	# xml += '<war:Byline>'
	# xml += '	<war:Text>?</war:Text>'
	# xml += '	<war:EditorId>?</war:EditorId>'
	# xml += '</war:Byline>'

		xml += '<war:VendorInformation>'+cgi.escape(json)+'</war:VendorInformation>'

		xml += '</war:BroadcastItem>'
		logger.debug("%d          XML built." % (hit_number))


	xml += '</war:BroadcastItems>'
	xml += '</war:InsertItemsRequest>'
	xml += '</soapenv:Body>'
	xml += '</soapenv:Envelope>'
	
	logger.info( ppids_in_xml)
	return xml


# given the SOAP response XML, update the PP records
def _set_ids (response_xml):
	global hits_soaped

	logger.debug( " -       - set_ids of %s" % (ITEMS_PER_SOAP_ENVEL))
	#logger.debug( " -       - set_ids of 5" + response_xml)
	doc = xml.dom.minidom.parseString(response_xml)
	itemlist = doc.getElementsByTagName('ns2:CreatedItem')
	if ( len( itemlist) > 0):
		conn = MySQLdb.connect(host = MYSQL_PP_HOST, user = MYSQL_PP_USER, passwd = MYSQL_PP_PWD, db = MYSQL_PP_DB)  # moved here 121016
		for i in itemlist:
			oid = i.getElementsByTagName('ns2:OriginalItemId')[0]
			nid = i.getElementsByTagName('ns2:CreatedItemId')[0]
			#121016 conn = MySQLdb.connect(host = MYSQL_PP_HOST, user = MYSQL_PP_USER, passwd = MYSQL_PP_PWD, db = MYSQL_PP_DB)

			this_hit_id = getText(oid.childNodes)
			newsitem_id = getText(nid.childNodes)
			sql = "UPDATE mdw_item SET mdw_id = '%s' where pinpoint_unique_id = '%s'" % (newsitem_id,this_hit_id)
			
			# queueKeyframe (newsitemid, keyframeUrl, hitId)
			if (kfqueue.has_key(this_hit_id)):
				_queueKeyframe (newsitem_id, kfqueue[this_hit_id], this_hit_id)

			hits_soaped = hits_soaped+1
			logger.debug( "%d %s" % (hits_soaped, sql))
			conn.query(sql)
	else:
		_soapReturnParse( response_xml, "")

# auto-correct items that may have already been delivered to MDW by looking up the NewsItemID and updating the db
def _check_existing ():
	global existing
	global bad_radio
	if ( len( existing) < 1 and len( bad_radio) < 1):
		return " "
   
	conn = MySQLdb.connect(host = MYSQL_PP_HOST, user = MYSQL_PP_USER, passwd = MYSQL_PP_PWD, db = MYSQL_PP_DB)
	if ( len( existing) > 0):  # 121024
		for i in existing:
			resp = _SOAP_POST('','<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:war="http://www.cision.com/monitoring/warehouse"> <soapenv:Header/> <soapenv:Body> <war:LookupNewsItemIdRequest> \
<war:OriginItemId>%s</war:OriginItemId> <war:SourceSystem>%d</war:SourceSystem> </war:LookupNewsItemIdRequest> </soapenv:Body> </soapenv:Envelope>' % (i,SOURCE_ID))
			rex = re.compile(r'.+<\w+:NewsItemId>(\d+)</\w+:NewsItemId>.+')
			m = rex.match(resp)
			if m:
				logger.debug("PPID %s previously delivered as NewsItemID %s" % (i, m.group(1)))
				sql = "UPDATE mdw_item SET mdw_id = '%s' WHERE pinpoint_unique_id = '%s' AND mdw_id IS NULL" % (m.group(1), i)
				logger.debug(sql)
				conn.query(sql)

				sql = "UPDATE mdw_item SET error_status = -4 WHERE pinpoint_unique_id = '%s'" % (i)
				conn.query(sql)
		existing = []

	if ( len( bad_radio) > 0):
		for i in bad_radio:
			sql = "UPDATE mdw_item SET error_status = -5 WHERE pinpoint_unique_id = '%s'" % (i)
			conn.query(sql)
		bad_radio = []





# take out characters that will mess up SOAP
# added 121206 from Web
def strip_control_characters(input):   
          
	if input:  
                  
		import re  
              
		# unicode invalid characters  
		RE_XML_ILLEGAL = u'([\u0000-\u0008\u000b-\u000c\u000e-\u001f\ufffe-\uffff])' + u'|' + \
		                 u'([%s-%s][^%s-%s])|([^%s-%s][%s-%s])|([%s-%s]$)|(^[%s-%s])' % \
                       (unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff),  
		                   unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff),  
		                   unichr(0xd800),unichr(0xdbff),unichr(0xdc00),unichr(0xdfff),  
		                   )  
		input = re.sub(RE_XML_ILLEGAL, "", input)  
                  
		# ascii control characters  
		input = re.sub(r"[\x01-\x1F\x7F]", "", input)  
                  
	return input  


# take out bad media IDs that were identified as missing their feedtypes
def _clear_bad_media_ids ():
	global bad_media_ids

	conn = MySQLdb.connect(host = MYSQL_PP_HOST, user = MYSQL_PP_USER, passwd = MYSQL_PP_PWD, db = MYSQL_PP_DB)
	for i in bad_media_ids:
		logger.warn(" BAD MEDIA ID %s" % (i))
		if (  len( i) < 2 or len(i) > 9):
			continue;
		sql = "UPDATE mdw_item SET mdw_id = 0 WHERE media_id = %s and mdw_id IS NULL" % (i)
		logger.debug(sql)
		conn.query(sql)

	bad_media_ids = []
	


# using "domain sockets"  - must import socket, sys, time
def get_lock(process_name):
	global lock_socket
	lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
	try:
		lock_socket.bind('\0' + process_name)
		logger.info( "   got the lock.")
	except socket.error:
		#logger.error("LOCK EXISTS - second instance denied, exiting.")
		logger.warn("LOCK EXISTS - second instance denied, exiting.")
		sys.exit()


#
# main
#

if __name__=='__main__':
	global existing
	global bad_radio
	global bad_media_ids
	global ppid_processed
	global bad_keyframes

	global hits_redundant, hits_bad_media_id, hits_bad_soap_envelope

	existing = []
	bad_media_ids = []
	bad_radio = []
	bad_keyframes = []
   

	hits_soaped = 0
	hit_number = 0
	ppid_processed = dict()
	hits_redundant =0
	hits_bad_media_id = 0
	hits_bad_soap_envelope = 0



# using "domain sockets:

#	get_lock('pinpoint_to_mdw')     # trying as of 121215


	pid = str(os.getpid())

	_init()
	_preprocess_pp()
	h = _get_hits()
	logger.info("Received %d hits for processing" % ( len(h), ))

	while len(h) > 0:
		thislist = h[0:ITEMS_PER_SOAP_ENVEL ]
		thisxml = strip_control_characters( _build_xml(thislist).replace( ' Estimated', ''))   # fix for bad character detected 121128

		logger.debug( " --------- next set of %d" % (ITEMS_PER_SOAP_ENVEL ))
		#logger.debug(thisxml)

		response = ""
		if thisxml.find('<war:BroadcastItem ') > 0:  # make sure at least one item is in the XML before sending
			try:
				response = _SOAP_POST('',thisxml)
				
				if response and response != " ":
					#logger.debug( " -       - set_ids of 10")
					#logger.debug( thisxml)   # 121128
					_set_ids (response)
				else:
					logger.debug( " -       - SOAP_POST FAIL %d" % (ITEMS_PER_SOAP_ENVEL ))
					logger.debug(thisxml)
	
				# error condition cleanup, if necessary
	
	
				for id in existing:
					logger.info( "       existing %s" % (id))
	
				for id in bad_radio:
					logger.info( "       bad radio %s" % (id))
	
				for id in bad_media_ids:
					logger.info( "       bad media id %s" % (id))
	
				_check_existing ()
				_clear_bad_media_ids ()
	
			except ValueError:
				#logger.error(" -       - Unable to process hits. %s" % (sys.exc_info()[1],))
				logger.warn(" -       - Unable to process hits. %s" % (sys.exc_info()[1],))
				logger.debug('response from MDW: ' + response)

		h[0:ITEMS_PER_SOAP_ENVEL ] = []
		#sys.exit()

	logger.info("      === ::: done, %s to soap --- %d redund %d bad media %d bad soap" % (hits_soaped, hits_redundant, hits_bad_media_id, hits_bad_soap_envelope))
	
