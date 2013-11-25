#!/usr/bin/python
# Zimbra backup per mailbox

import os, sys, getopt
import ldap
import subprocess
import datetime
import time
import re
import shutil
import urllib2
from pynsca import NSCANotifier
import requests
import logging
import mmap

# config START
## LDAP Auth
auths= {
	# eg. "mailserver.example.com":"ldapsecret",
	"FQDN_OF_MAILSERVER_1":"PASSWORD",
	"FQDN_OF_MAILSERVER_2":"PASSWORD"
}

## Zimbra admin login. Login for https://mailserver.example.com:7071
zimbraauths= {
	# eg. "mailserver.example.com":"adminsecret"
	"FQDN_OF_MAILSERVER_1":"PASSWORD",
	"FQDN_OF_MAILSERVER_2":"PASSWORD"
}

## OPTIONAL: NSCA enabled (passive checks) nagios server config
nagioshosts= {
	# eg. "mailserver.example.com":"monitor.sub.example.com",
	"FQDN_OF_MAILSERVER_1":"FQDN_OF_NAGIOS_SERVER_1",
	"FQDN_OF_MAILSERVER_2":"FQDN_OF_NAGIOS_SERVER_2"
}

## OPTIONAL: Nagios service. The passive check enabled service configured in nagios
service="mailbox_backups"

## Backup directory. Where the backups go.
mailbackuptopdir="/mailbackups/"

## Logfile. Add to logrotate?
logfile='/var/log/zimbra_permailbox_backup.log'

# config END

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=logfile,level=logging.DEBUG)

import commands
class singleinstance(object):
    '''
    singleinstance - based on Windows version by Dragan Jovelic this is a Linux
                     version that accomplishes the same task: make sure that
                     only a single instance of an application is running.

    '''
                        
    def __init__(self, pidPath):
        '''
        pidPath - full path/filename where pid for running application is to be
                  stored.  Often this is ./var/<pgmname>.pid
        '''
        self.pidPath=pidPath
        #
        # See if pidFile exists
        #
        if os.path.exists(pidPath):
            #
            # Make sure it is not a "stale" pidFile
            #
            pid=open(pidPath, 'r').read().strip()
            #
            # Check list of running pids, if not running it is stale so
            # overwrite
            #
	    try:
		os.kill( int(pid), 0)
		pidRunning = 1
	    except OSError:
		pidRunning = 0
            if pidRunning:
                self.lasterror=True

            else:
                self.lasterror=False

        else:
            self.lasterror=False

        if not self.lasterror:
            #
            # Write my pid into pidFile to keep multiple copies of program from
            # running.
            #
            fp=open(pidPath, 'w')
            fp.write(str(os.getpid()))
            fp.close()

    def alreadyrunning(self):
        return self.lasterror

    def __del__(self):
        if not self.lasterror:
            os.unlink(self.pidPath)

def date_valid(yyyymmdd):
	try:
		datetime.datetime.strptime(yyyymmdd,'%Y%m%d')
	except ValueError:
		return False

def file_exists_and_non_zero(file):
	try:
		os.path.getsize(file) > 0
	except:
		return False
	return True

def find_files_matching(name, path):
	result = []
	for root, dirs, files in os.walk(path):
		if name in files:
			result.append(os.path.join(root, name))
	return result

def file_size(file):
	if file_exists_and_non_zero(file):
		return os.path.getsize(file)

def sizeof_fmt(num):
    for x in ['bytes','KB','MB','GB','TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

def zimbra_backup_per_mailbox(site,mailaccount,outputdir):
	today = str(datetime.datetime.now().date()).replace('-','')
	backupdir=outputdir+today
	if not os.path.isdir(backupdir):
		os.makedirs(backupdir)
	output=backupdir+'/'+mailaccount+'.tgz'
	global busywith
	busywith=output
	url='https://'+site+':7071/home/'+mailaccount+'/?fmt=tgz'
	logging.info("Account: "+mailaccount+". Backup started using "+url+" to "+output)
	maxtries = 20
	delay = 20
	tries = 0
	while tries < maxtries:
		try:
			passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
			passman.add_password(None, url, "admin", zimbraauths[site])
			urllib2.install_opener(urllib2.build_opener(urllib2.HTTPBasicAuthHandler(passman)))
			req = urllib2.Request(url)
			dl = urllib2.urlopen(req, timeout=60)
			with open(output, 'wb') as fp:
	       			shutil.copyfileobj(dl, fp)
		except urllib2.URLError:
        		tries += 1
		        time.sleep(delay)
			logging.info("Account: "+mailaccount+". Problem connecting during backup. Attempts: "+str(tries))
		except Exception, error:
    			logging.exception(error)
        		tries += 1
		        time.sleep(delay)
    		else:
        		break
	if tries == 10:
		return False	

	if file_exists_and_non_zero(output):
		size=str(sizeof_fmt(file_size(output)))
		fails=str(tries)
		logging.info("Account: "+mailaccount+" Backup succeeded. Size: "+size+". Failed attempts: "+fails)
		return True
	else:
		return False

def zimbra_retrieve_all_accounts(site,binddn,passwd):
	formatoutput=[]
        try:
        	l=ldap.initialize("ldap://"+site)
	        l.simple_bind_s(binddn,passwd)
                search=l.search_s("",ldap.SCOPE_SUBTREE, "(zimbraMailHost="+site+")", ["zimbraMailDeliveryAddress"])
        except:
		error="Retrieving Zimbra accounts from "+site+" failed"
                print error
		logging.info(error)
                return False
        for item in search:
		if item[1].values():
			formatoutput.extend(item[1].values()[0])
			
	if not formatoutput:
		logging.info("Unable to retrieve any accounts from LDAP - auth issue?")
		print "Unable to retrieve any accounts from LDAP - auth issue?"
		sys.exit(1)
	else:
		return formatoutput

def zimbra_backupall(site,report):
	logging.info("Starting full backup of site: "+site)
	# retrieve all valid accounts
	accounts=zimbra_retrieve_all_accounts(site,"uid=zimbra,cn=admins,cn=zimbra",auths[site])
	numaccounts=len(accounts)
	numbackedup=0
	sizebackedup=0
	firstfailedacc=''
	failedcount=0
	# do the backups
	if accounts:
		accounts.sort()
		for account in accounts:
			if zimbra_backup_per_mailbox(site,account,mailbackupdir):
				numbackedup += 1
				sizebackedup += file_size(busywith)
			else:
				if failedcount == 0:
					firstfailedacc=account
				else:
					failedcount += 1

	# report to Nagios or to terminal
	percent=percentage(numbackedup,numaccounts)
	msg="Backups ("+str(numbackedup)+" of "+str(numaccounts)+") "+str(percent)+"% completed with size: "+str(sizebackedup)+" ("+sizeof_fmt(sizebackedup)+")"
	logging.info(msg+" - first failed backup: "+str(firstfailedacc))
	if report == "yes":
		if numaccounts == numbackedup:
			if nagios_passive_report(nagioshosts[site],site,service,0,msg):
				sys.exit(0)
			else:
				print "Failed to send report to nagios host: "+nagioshosts[site]+"\n"+msg
				sys.exit(1)
		else:
			if percent < 95:
				status=2
			else:
				status=1

			if nagios_passive_report(nagioshosts[site],site,service,status,msg+" - first failed backup: "+str(firstfailedacc)+" Log: "+nagioshosts[site]+":"+logfile):
                                sys.exit(0)
                        else:  
                                print "Failed to send report to nagios host: "+nagioshosts[site]+"\n"+msg
                                sys.exit(1)
			print msg
			sys.exit(1)
	else:
		if numaccounts == numbackedup:
			print msg
			sys.exit(0)
		else:
			print msg+" - first failed backup: "+firstfailedacc
			sys.exit(1)

def percentage(part, whole):
	return 100 * float(part)/float(whole)
				
def nagios_passive_report(host,monitoredhost,service,level,message):
	# use nsca to report to nagios
	status={0:"OK: ",1:"WARNING: ",2:"CRITICAL: "}
	try:
		notif = NSCANotifier(host,5667,0)
		notif.svc_result(monitoredhost, service, level, status[level]+message)
	except:
		return False
	return True

def zimbra_account_exists(site,account):
	accounts=zimbra_retrieve_all_accounts(site,"uid=zimbra,cn=admins,cn=zimbra",auths[site])
	if account in accounts:
		return True
	else:
		return False

def zimbra_backup_account(site,account):
	if zimbra_account_exists(site,account):
		zimbra_backup_per_mailbox(site,account,mailbackupdir)
	else:
		print account+" does not exist on "+site
		return False

def zimbra_restore_account(site,date,mailaccbackup,restoreaccount):
	backuptorestore=mailbackupdir+date+"/"+mailaccbackup+".tgz"
	if zimbra_account_exists(site,restoreaccount):
		if file_exists_and_non_zero(backuptorestore):
			msg="Running a restore to account "+restoreaccount+" on "+site+" using backup "+backuptorestore
			print msg
			logging.info(msg)
			upload=open(backuptorestore, 'rb')
			try:
				mmapped_file_as_string = mmap.mmap(upload.fileno(), 0, access=mmap.ACCESS_READ)
	                        url = 'https://'+site+':7071/home/'+restoreaccount+'/?fmt=tgz&resolve=skip'
        	                do_it = requests.post(url, data=mmapped_file_as_string, auth=('admin', zimbraauths[site]))
			except ValueError as e:
				print e
				print "Possible Memory issue! If you restoring large mailboxes you probably need to run this from a 64bit machine!"
				sys.exit(1)
			if do_it.status_code == 200:
				print "Restore complete"
			else:
				errmsg="Restore failed with http error code: "+str(do_it.status_code)
				print errmsg
				logging.info(errmsg)
	
		else:
			print backuptorestore+" does not exist or is empty"
			sys.exit(1)
	else:
		print "The account "+restoreaccount+" does not exist on "+site
		sys.exit(1)
	return True

def zimbra_list_restores(site,mailaccount):
	files=find_files_matching(mailaccount+".tgz",mailbackupdir)
	print "Backup(s) for account: "+mailaccount+" in "+mailbackupdir
	for file in files:
		print "Date: "+file.split("/")[-2]+", Size: ",sizeof_fmt(file_size(file))

def zimbra_delete_old_backups(site):
	backupstokeep =[]
	existingbackups=[]
	today = datetime.datetime.now().date()
	
	# the date 1 month ago
	onemonthago = today - datetime.timedelta(days=31)
	# the date 6 months ago
	sixmonthsago = today - datetime.timedelta(days=180)
	# the date 1 week ago
	oneweekago = today - datetime.timedelta(days=7)
	# all days between oneweekago and today
	dailys = date_range(oneweekago,today)
	# all days between one month ago and one week ago
	weeklys = date_range(onemonthago,oneweekago)
	# all days between six months ago and one month ago
	monthlys = date_range(sixmonthsago,onemonthago)

	# place the last 7 days in a list
	for day in dailys:
		backupstokeep.extend([day.replace('-','')])

	# place all days between 1 month and 1 week ago ending with certain days in a list
	for day in weeklys:
		if day.endswith(("01","02","03","09","10","11","16","17","18","23","24","25")):
			backupstokeep.extend([day.replace('-','')])

	# place all days between 6 months and 1 month ago ending with certain days in a list
	for day in monthlys:
		if day.endswith(("01","02","03")):
			backupstokeep.extend([day.replace('-','')])
	
	# get a list of existing backups
	for backup in file_regex_list(mailbackupdir,"......"):
		 existingbackups.extend([backup.split("/")[-1]])

	# if backupstokeep is not empty then delete all backups that aren't in the backupstokeep list
	if len(backupstokeep) > 30:
		for backup in existingbackups:
			if not backup in backupstokeep:
				shutil.rmtree(mailbackupdir+backup)

def date_range(start_date, end_date):
	datelist=[]
	delta = datetime.timedelta(days=1)
	while start_date <= end_date:
		datelist.extend([start_date.strftime("%Y-%m-%d")])
		start_date += delta
	return datelist

def file_regex_list(dir,regex_search):
        """ Specify directory and regex_search. All files matched are returned in a list """
        list=[]
        files = os.listdir(dir)
        for file in files:
                if re.search(regex_search, file):
                        list.extend([dir+file])
        return list

def file_list_all_in_dir_recursively(dir):
	filelist=[]
	for root, subFolders, files in os.walk(dir):
		for file in files:
			filelist.append(os.path.join(root,file))
	return filelist
	
# Help
def main(argv):
	helptext='Usage:\n\t\
	-b [all|"bob@domain"] \t# backup mail account\n\t\
	-r "bob@domain" \t\t# account to restore backup from\n\t\
	-t "bobrestore@domain" \t# account to restore to\n\t\
	-z \t\t\t\t# only valid for "-b all" - report status to nagios\n\t\
	-s [MAILSERVER1|MAILSERVER2] \t# site name\n\t\
	-d ["20130606"|list] \t\t# date - used to specify which backup to restore\n\t\t\t\t\t\
	# or "list" to output all possible restores for specified account\n\t\
	-x \t\t\t\t# delete old backups - we keep 6 monthlys, 4 weeklys, 7 dailys per mailbox.\n\t\
	-h # help\n\n\
	eg. '+(__file__)+' -b bob@domain -s MAILSERVER1\n\
	eg. '+(__file__)+' -b all -s MAILSERVER1 -z\n\
	eg. '+(__file__)+' -r bob@domain -t bobrestore@domain -d 20130905 -s MAILSERVER1\n\
	eg. '+(__file__)+' -r bob@domain -d list -s MAILSERVER1\n'

	date=''
	mailaccount=''
	method=''
	mailserver=''
	report='no'
	backupall=''
	restoreaccount=''
	try:
		opts, args = getopt.getopt(argv,"hb:r:azs:d:t:x")
	except getopt.GetoptError:
		print helptext
		sys.exit(1)
	if not opts:
		print helptext
		sys.exit(1)
	for opt, arg in opts:
		if opt == '-h':
			print helptext
			sys.exit()
		elif opt in ('-s'):
			if arg in auths.keys():
				mailserver=arg
				global mailbackupdir
				mailbackupdir=mailbackuptopdir+mailserver.split(".")[0]+"/mailboxes/"
			else:
				print "Invalid site "+"'"+arg+"'"+"\n\n"+helptext
				sys.exit(1)
		elif opt in ('-d'):
			if arg == "list":
				method="restorelist"
			elif date_valid(arg) != False:
				date=arg
			else:
				print "Invalid date "+"'"+arg+"'"+"\n\n"+helptext
				sys.exit(1)
		elif opt in ('-b'):
			method="backup"
			if arg == "all":
				method = "backupall"
			else:
				mailaccount=arg
		elif opt in ('-r'):
			method="restore"
			mailaccount=arg
		elif opt in ('-t'):
			restoreaccount=arg
		elif opt in ('-z'):
			report="yes"
		elif opt in ('-x'):
			method="deleteoldbackups"
	if not method or not mailserver:
		print "You need to specify a method (-b/-r) and site.\n\n"+helptext
		sys.exit(1)
	elif method == "restore":
		if not restoreaccount:
			print "You need to specify an account to restore to.\n\n"+helptext
			sys.exit(1)
		elif not mailaccount:
			print "You need to specify an account to restore a backup from.\n\n"+helptext
			sys.exit(1)
		elif not date:
			print "You need to specify a date when restoring an account\n\n"+helptext
			sys.exit(1)
		else:
			zimbra_restore_account(mailserver,date,mailaccount,restoreaccount)
	elif method == "restorelist":
                if not mailaccount:
                        print "You need to specify an account to list restore points.\n\n"+helptext
                        sys.exit(1)
                else:
			zimbra_list_restores(mailserver,mailaccount)
	elif method == "backup":
		if not mailaccount:
			print "You need to specify a mailaccount to backup\n\n"+helptext
                        sys.exit(1)
		else:
			zimbra_backup_account(mailserver,mailaccount)
	elif method == "backupall":
		backupall = singleinstance('/var/run/zimbra_'+method+'.pid')
		if backupall.alreadyrunning():
			errmsg="Another instance of this program is already running"
			logging.info(errmsg)
			sys.exit(errmsg)
		else:
			zimbra_backupall(mailserver,report)
	elif method == "deleteoldbackups":
		deleteoldbackups = singleinstance('/var/run/zimbra_'+method+'.pid')
		if deleteoldbackups.alreadyrunning():
			errmsg="Another instance of this program is already running"
                        logging.info(errmsg)
                        sys.exit(errmsg)
		else:
			zimbra_delete_old_backups(mailserver)
	else:
		print "Don't know what you want to do...\n\n"+helptext
		sys.exit(1)

if __name__ == "__main__":
	main(sys.argv[1:])
