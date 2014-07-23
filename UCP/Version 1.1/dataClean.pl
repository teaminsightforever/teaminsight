#!/usr/bin/perl
use strict;
#use warnings;
use File::Copy;

###### CONFIGURATION ##################
## Ensure Perl is installed on Slave servers ##

my $servers = "lon5dpu277:20140201>20140228,lon5dpu272:20140301>20140331,lon5dpu271:20140401>20140430,lon5dpu273:20140501>20140531,lon5dpu275:20140601>20140630,lon5dpu278:20140701>20140711,lon5dpu279:20130601>20130630,lon5dpu276:20130701>20130731,lon5dpu264:20130801>20130831,lon5dpu265:20130901>20130930,lon5dpu280:20131001>20131031,lon5dpu263:20131101>20131130,lon5dpu262:20131201>20131231"; # Specify the servers and the dates they should run. syntax server1:staredate>enddate,server2:staredate>enddate,
my $idFieldNum = 236; #specify the field number for the unique identifier in the SiteCat files.
my $fileLocation = "//lon5nas101/tesco/Feeds/SiteCat/tescoukgroceriesprod/staging/"; # Raw SiteCat files
my $finalDCFileLocation = "//lon5nas101/tesco/UCP_Test/UCP_Test_Output_DC/"; #Main DC output location
my $finalLUFileLocation = "//lon5nas101/tesco/UCP_Test/UCP_Test_Output_LU/"; #Main LU output location
my $finalDoneFileLocation = "//lon5nas101/tesco/UCP_Test/UCP_Test_Done/"; #Main Done output location
my $serverDoneFileLocation = "//lon5nas101/tesco/UCP_Test/UCP_Server_Done/"; #Server Done output location - tells the master script when each slave has finished
my @columnsPassed = (0,1,3,4,7,8,9,10,11,12,13,15,16,17,18,19,20,28,31,32,33,37,38,39,40,41,48,51,52,53,54,55,57,60,61,62,63,64,65,66,67,68,69,70,71,77,82,90,91,92,97,98,99,101,103,144,145,146,148,149,150,156,157,165,170,177,178,186,188,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,219,220,221,234,236,253,265,268,269,273,281,288,289,290,291,296,301,302,303,304,307,308,313,314,315,316,329,330,331,333,335,336,337,338,339,342,343,344,345,353,356,357,358,359,360,361,362,363,364);
my $logDirectory = "E:\\Scripts\\UCPMaster\\";

###### END OF CONFIG ########################

my $serverType = "None";
$serverType = $ARGV[0];
my $startDate = "None";
$startDate = $ARGV[1];
my $endDate = "None";
$endDate = $ARGV[2];
my $serverName = "None";
$serverName = $ARGV[3];

my $rootDirectory = "E:\\Scripts\\UCPSlave\\"; 
my $stagingDirectory = $rootDirectory."Staging\\";
my $dcPhase1Directory = $rootDirectory."DC_Phase1\\";
my $dcOutputDirectory = $rootDirectory."DC_Output\\";
my $luOutputDirectory = $rootDirectory."LU_Output\\";

my @inputFiles;

if($serverType eq "Master")
{
	print $serverType;
	my @serverCount = split(',',$servers);
	foreach(@serverCount)
	{
		my $string = $_;
		$string =~ /(.*):(.*)>(.*)/;
		my $server = $1;
		my $startDate = $2;
		my $endDate = $3;
	
		# Step 1: Copy Perl Scripts / Make Directories on Slave
		
		my $loc = "\\\\$server\\e\$\\Scripts\\UCPSlave\\";
		rmdir $loc;
		mkdir $loc unless -d $loc;
	
		mkdir "\\\\$server\\e\$\\Scripts\\UCPSlave\\Staging\\";
		mkdir "\\\\$server\\e\$\\Scripts\\UCPSlave\\DC_Phase1\\";
		mkdir "\\\\$server\\e\$\\Scripts\\UCPSlave\\DC_Output\\";
		mkdir "\\\\$server\\e\$\\Scripts\\UCPSlave\\LU_Output\\";
	
		`copy "dataClean.pl" "$loc"` or die "Copy failed: $!";
		`copy "runSlave.bat" "$loc"` or die "Copy failed: $!";
		`copy "gzip.exe" "$loc"` or die "Copy failed: $!";
		addToLog("Activating $server : $serverType | Start Date: $startDate | End Date: $endDate");
		`PsExec.exe \\\\$server -s -d E:\\Scripts\\UCPSlave\\runSlave.bat "Slave" "$startDate" "$endDate" "$server" -w "E:\\Scripts\\UCPSlave\\"`;
	}
	
	# Enter into loop to wait for all of the servers to complete
	my $complete = "false";
	#while($complete eq "false")
	#{
	#	my @serverDoneFiles = ScanDirectory($serverDoneFileLocation);
	#	my @serverCount = split(',',$servers);
	#	my $doneFileCount = scalar @serverDoneFiles;
	#	my $serverCount = scalar @serverCount;
	#	if($doneFileCount == $serverCount) {
	#		$complete = "true";
	#	}
	#}
	
	# my @lookupFiles = ScanDirectory($finalLUFileLocation,'.txt');
	#
	# Merge all lookups together
	#my %hash;
	#my $key;

	#foreach(@lookupFiles)
	#{
		#open (DATA,$finalLUFileLocation.$_);
		#while (<DATA>) {
		#	my @row = split;
		#	if (@row > 1) {
		#		$key = shift @row;
		#	} else {
		#		push @{$hash{$key}}, shift @row;
		#	}
		#}
		#close (DATA);
	#}
}

if($serverType eq "Slave")
{	
	$logDirectory = "E:\\Scripts\\UCPSlave\\";
	unlink($logDirectory."Log.txt");
	addToLog("Server Type: $serverType | Start Date: $startDate | End Date: $endDate");

	#copyFiles($fileLocation,$stagingDirectory,$startDate,$endDate);
	
	#Test to see what files are in ScanDirectory
	my @nasFiles = ScanDirectory($fileLocation,'.tsv.gz',$startDate,$endDate);
	my $filename = "";
	
	foreach(@nasFiles)
	{
		addToLog("about to process $_");
	}
	
	foreach(@nasFiles)
	{
		next unless checkExists($finalDoneFileLocation,$_) == 0;
		addToLog("Copying File From: $fileLocation $_ TO $stagingDirectory $_");
		copy($fileLocation.$_,$stagingDirectory.$_);
		$filename = $_;
		my $filesize = -s $stagingDirectory.$_;
		print "Begin Unzipping File: ".$_."\n";
		
		addToLog ("Begin Unzipping File: ".$_." Size: $filesize ");
		gzipFile($stagingDirectory.$_,$dcPhase1Directory.$_);
		$filesize = -s $dcPhase1Directory.$_.".txt";
		print "Finished Unzipping File: ".$_."\n";
		addToLog ("Finished Unzipping File: $_ Size: $filesize");

		#Create output file for Data Clean
		open (DC_OUTFILE, '>>', $dcOutputDirectory.$_.".txt");
		#Create output file for Lookup
		open (LU_OUTFILE, '>>', $luOutputDirectory.$_.".txt");
		
		#Open the Input File and also copy name so can delete later
		open (INFILE,$dcPhase1Directory.$_.".txt");
		
		my $dcPhase1FileName = $dcPhase1Directory.$filename.".txt"; #Create full directory path
		my $luPhase1FileName = $luOutputDirectory.$filename.".txt"; #Create full directory path
		my $dcOutputFileName = $dcOutputDirectory.$filename.".txt"; #Create full directory path
		my $zipPhase1FileName = $stagingDirectory.$filename; #Create full directory path
		
		#Initiate HashMap
		my %SCID_Evar_Mapping = ();
		#Read in file and clean and also output phase two lookups (one for each hour)
		print "Begin Processing File: $dcPhase1FileName \n";
		addToLog ("Begin Processing File: $dcPhase1FileName");
		
		while (<INFILE>) {
			chomp;
			my @f = split("\t");

			#This bit of logic decides if the column should be printed
			#Only progress if this part is true, needs to be part of a modular function
			if($f[0] == 1 || ($f[0] == 2 && $f[203])){ 		
				#Columns should be the number of columns in the output
				my $columns = scalar(@columnsPassed);
				for(my $i = 0; $i < $columns; $i ++ ){
					print DC_OUTFILE "$f[$columnsPassed[$i]]\t";
				}
				print DC_OUTFILE "\n"; #Finish up the line
			}
			
			#This section outputs the SCID to Evar53 to a hash-map.
			if($f[236]!=""){
				$SCID_Evar_Mapping{"$f[8]-$f[9]"} = $f[236];
				#Add pair to hash-map
			}	
		}	
		close (INFILE);
		
		#After complete delete file
		print "Finished Cleaning File: ".$dcPhase1FileName."\n";
		addToLog ("Finished Cleaning File: $dcPhase1FileName");
		

	#After closing the INFILE output the HashMap to a file.
			while ( my ($key, $value) = each(%SCID_Evar_Mapping) ) {
			print LU_OUTFILE "$key\t$value\n";
		}
		addToLog ("Write Lookup File: $_");
		
	close (DC_OUTFILE);
	close (LU_OUTFILE);

	#once complete, copy the file to the final destination (NAS)
	copy ($dcOutputFileName,$finalDCFileLocation);
	addToLog ("Copying DC to final destination: $dcOutputFileName To $finalDCFileLocation");
	copy ($luPhase1FileName,$finalLUFileLocation);
	addToLog ("Copying LU to final destination: $luPhase1FileName To $finalLUFileLocation");
	
	#create done files (NAS)
	createDoneFile($finalDoneFileLocation.$filename);
	addToLog ("Create done file: $finalDoneFileLocation$filename.done");
	
	#cleaning files once complete
	unlink($dcPhase1FileName);
	addToLog ("Cleaning DC File: $dcPhase1FileName");
	unlink($luPhase1FileName);
	addToLog ("Cleaning LU File: $luPhase1FileName");
	unlink($zipPhase1FileName);
	addToLog ("Cleaning ZIP File: $zipPhase1FileName");
	unlink($dcOutputFileName);
	addToLog ("Cleaning DC Output File: $zipPhase1FileName");
	}
	
	addToLog ("Process Complete");
	createDoneFile($serverDoneFileLocation.$serverName);
}



##############################Functions############################
sub ScanDirectory{
    my ($workdir) = shift;
	my ($extension) = shift;
	my ($startDate) = shift;
	my ($endDate) = shift;
	
    opendir(DIR, $workdir) or die "Unable to open $workdir\n $!\n";
    my @names = readdir(DIR) or die "Unable to read $workdir\n $!\n";
    closedir(DIR);
    my @return = "";

    foreach my $name (@names){
	   # use start and end date filtering if present.
	   my $fileDate = $name; 
	   $fileDate =~ /(\d{8})/;
	   $fileDate = $1;
	   if($startDate != "" && $endDate != "") {
	   next unless($fileDate >= $startDate && $fileDate <= $endDate)
	   }
	   
	   # Use a regular expression to ignore files beginning with a period
       next if ($name =~ m/^\./);
       next unless $name =~ /(.*$extension$)/;
		  #print $name."\n";
		  push(@return, $name);
    }
	return @return;
}

sub gzipFile {
  my $inFile = shift;
  my $outFile = shift;
  
  system("E:\\Scripts\\UCPSlave\\gzip.exe -dc $inFile > $outFile.txt");
  if ($? == -1) {
    print "failed to execute unzip: $!\n";
  }
}

sub addToLog
{
	my ($logrow) = shift;
	my $timestamp = scalar localtime();
	open OUT, '>>', $logDirectory."Log.txt" or die;
	print OUT  $timestamp." - ".$logrow."\n";
	close OUT;	
}

sub copyFiles()
{
	my $from = shift();
	my $to = shift();
	my $startDate = shift();
	my $endDate = shift();
	
	my @zipFiles = ScanDirectory($from,'.tsv.gz');
	
	foreach(@zipFiles)
	{
		next if ($_ eq "");
		my $fileDate = $_; 
		$fileDate =~ /(\d{8})/;
		$fileDate = $1;
		
		if($fileDate >= $startDate && $fileDate <= $endDate){
			my $fromFile = $from.$_;
			my $toFile = $to.$_;		
			if (-e $to.$_) {
				print "File Already Exists: $_ \n";
			} else {
				print "Copying File: $_ \n";
				addToLog ("Copying File: $_");
				copy ($fromFile,$toFile) or warn "Copy failed: $! \n";
			}
		}
	}
}

sub createDoneFile {
	my $path = shift;
	if ($path ne "")
	{
			open(FH,">".$path.".done") or warn "$!";
			close FH;
	}
}

sub checkExists
{
	my $directory = shift;
	my $input = shift;
	my $output = "";
	
	my @doneFiles = ScanDirectory($directory);
	
	#set the input to ignore any file extensions
	if ($input =~ m/(\w*-\w*)/)
	{
		$input = $1;
	}
	
	foreach (@doneFiles){
		#print "INPUT: ".$input."\n";
		#print "LOOP: ".$_."\n";
		
		if ($_ =~ m/(\w*-\w*)/)
		{
			$output = $1;
		}
		
		 if ($input eq $output){
			addToLog ("Ignoring File: $input already exists");
			 #file exists
			 return 1;
		 }
	}
	
	return 0;
	
}