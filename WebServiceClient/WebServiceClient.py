from Bio import SeqIO
from collections import defaultdict
from recordclass import recordclass
import time
import urllib
from IPython.display import clear_output

def sendJob(fasta):
    url="http://bioware.ucd.ie/rest/ppIIpredPolled?fasta_sequence=" + str(urllib.quote_plus(fasta)) + "&aln=f&geneatealn=f&uniprotid=&strict=t&json=t&php=t"
    print url
    return urllib.urlopen(url).read()

def pollJob(jobId):
    url="http://bioware.ucd.ie/rest/checkJobStatus?jobid=" + str(urllib.quote_plus(jobId))
    return urllib.urlopen(url).read()


def jobsRemain(allJobsDict):
    for jobName in allJobs:
        currentJob=allJobs[jobName]
        if 'finished' not in currentJob.status:
            return True
    return False
    


class Job:
     def __init__(self, id_, seqRecord, fastaSequence, ppIISequence, iupredSequence, status, jobId):
        self.id_ = id_
        self.seqRecord = seqRecord
        self.fastaSequence = fastaSequence
        self.ppIISequence = ppIISequence
        self.iupredSequence = iupredSequence
        self.status = status
        self.jobId = jobId
        
        
def collectResults(job):
    print job
    url="http://bioware.ucd.ie/~compass/Datasets/ppIIpred/" + str(job.jobId) + "/input.ppIIpred"
    ppIIPredScores=urllib.urlopen(url).read()
    print ppIIPredScores
          
    
    

allJobs=defaultdict()
completeJobs=defaultdict()
record_dict = SeqIO.to_dict(SeqIO.parse("examples/input.fasta", "fasta"))
for record in record_dict:
    seqRecord = record_dict[record]
    fastaString=">" + seqRecord.id + "\n" + seqRecord.seq
    job = Job(seqRecord.id, seqRecord, fastaString, None, None, 'unsent', None)
    allJobs[seqRecord.id]=job

runningCounter=0
while jobsRemain(allJobs):
    #clear_output()
    print "---------------------------------------"
    for jobName in allJobs:
        currentJob=allJobs[jobName]
        
        if currentJob.status == 'unsent' and runningCounter < 3:
            runningCounter+=1
            id_=sendJob(currentJob.fastaSequence)
            currentJob.jobId=id_[2:-3]
            currentJob.status = 'submitted'
        
        if currentJob.status == 'submitted' or currentJob.status == "(\"running\");":
            currentJob.status=pollJob(currentJob.jobId)
            if 'finished' in currentJob.status:
                runningCounter=runningCounter-1
                collectResults(currentJob)
            
        print jobName + "\t", currentJob.jobId, currentJob.status
    time.sleep(10)
