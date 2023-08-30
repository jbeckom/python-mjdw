import os
import time 

LOGPATH = r'C:\Python\logs'
DAYS = 90
SECONDS = 86400

def scrub_aged_logs():
    # enumerate all sub-directories and files within LOGPATH
    for a in os.walk(LOGPATH):
        # delete empty directory/ies
        if os.path.isdir(a[0]) and len(os.listdir(a[0])) == 0:
            os.rmdir(a[0])
        # enumerate files within sub-directories
        for b in a[2]:
            fileLoc = os.path.join(a[0],b)
            # delete files older than N DAYS
            if (time.time()-os.stat(fileLoc).st_mtime) > (DAYS*SECONDS):
                os.remove(fileLoc)
            # one last check to delete empty directory/ies
            if len(os.listdir(a[0])) == 0:
                os.rmdir(a[0])

def main():
    scrub_aged_logs()

if __name__ == '__main__':
    main()

