#!/usr/bin/perl
########## UCP Script v1.24. - Developed by Craig Fitches & Phillip Law 21/7/2014 ##########
########## The main purpose of this script is to attached a unique identifier to every row of the Adobe SiteCat data where relevant. ##########
########## The script first unpacks the SiteCat files, cleans the files, loops through to extract the unique cookie and ID and rewrites the files.
########## This script can be configured to run across multiple servers ##########
########## Ensure Perl is installed on Slave servers ##########

use strict;
#use warnings;
use File::Copy;
use POSIX qw/strftime/;
use List::Util qw( min max );

# Accept Arguements
my $serverType = "None";
$serverType = $ARGV[0];
my $startDate = "None";
$startDate = $ARGV[1];
my $endDate = "None";
$endDate = $ARGV[2];
my $serverName = "None";
$serverName = $ARGV[3];

# Setup configuration locations based on server type
my $slaveLocation = "E:\\Scripts\\UCPSlave\\UCP.cfg";
my $masterLocation = "E:\\Scripts\\UCPMaster\\UCP.cfg";
my $mainLoc = $masterLocation;

if($serverType eq "Slave") {
	$mainLoc = $slaveLocation;
}

###### BEGIN CONFIG ########################

my $servers = GetSetting('servers',$mainLoc);
my $idFieldNum = GetSetting('idFieldNum',$mainLoc);
my $fileLocation = GetSetting('fileLocation',$mainLoc);
my $finalDCFileLocation = GetSetting('finalDCFileLocation',$mainLoc);
my $finalLUFileLocation = GetSetting('finalLUFileLocation',$mainLoc);
my $finalLUCombinedLocation = GetSetting('finalLUCombinedLocation',$mainLoc);
my $finalDoneFileLocation = GetSetting('finalDoneFileLocation',$mainLoc);
my $serverDoneFileLocation = GetSetting('serverDoneFileLocation',$mainLoc);
my @columnsPassed = GetSetting('columnsPassed',$mainLoc);
my $logDirectory = GetSetting('logDirectory',$mainLoc);
my $cleanServersAfter = GetSetting('cleanServersAfter',$mainLoc);
my $createMasterLookupFile = GetSetting('createMasterLookupFile',$mainLoc);
my $cleanData = GetSetting('cleanData',$mainLoc);
my $lookupDays = GetSetting('lookupDays',$mainLoc);

###### END OF CONFIG ########################

# setup baseline folder directory for slave machine
my $rootDirectory = "E:\\Scripts\\UCPSlave\\"; 
my $stagingDirectory = $rootDirectory."Staging\\";
my $dcPhase1Directory = $rootDirectory."DC_Phase1\\";
my $dcOutputDirectory = $rootDirectory."DC_Output\\";
my $luOutputDirectory = $rootDirectory."LU_Output\\";

# varibles
my @inputFiles;

if($serverType eq "Master")
{
	addToLog("\nservers:$servers\nidFieldNum:$idFieldNum\nfileLocation:$fileLocation\nfinalFileLocation:$finalLUFileLocation\nfinalDCFileLocation:$finalDCFileLocation\nfinalLUFileLocation:$finalLUFileLocation\nfinalLUCombinedLocation:$finalLUCombinedLocation\nfinalDoneFileLocation:$finalDoneFileLocation\nserverDoneFileLocation:$serverDoneFileLocation\ncolumnsPassed:@columnsPassed\nlogDirectory:$logDirectory\ncleanServersAfter:$cleanServersAfter\ncreateMasterLookupFile:$createMasterLookupFile\ncleanDate:$cleanData\nlookupDays:$lookupDays");

	open STDOUT, '>', "STDOUT.log";
	my @serverCount = split(',',$servers);
	foreach(@serverCount)
	{
		next if $_ eq "";
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
		`copy "UCP.cfg" "$loc"` or die "Copy failed: $!";
		addToLog("Activating $server : $serverType | Start Date: $startDate | End Date: $endDate");
		`PsExec.exe \\\\$server -s -d E:\\Scripts\\UCPSlave\\runSlave.bat "Slave" "$startDate" "$endDate" "$server" -w "E:\\Scripts\\UCPSlave\\"`;
	}
	
	addToLog("All servers started processing");

	if ($createMasterLookupFile eq "true")
	{	
		addToLog("Entering create combined master lookup");
		# Enter into loop to wait for all of the servers to complete
		# clean up the directory incase server done files exsits.
		my @serverDoneFiles = ScanDirectory($serverDoneFileLocation,'.done');
		foreach(@serverDoneFiles){
			addToLog("Deleting: $_");
			unlink $serverDoneFileLocation.$_;
		}
		
		addToLog("Entering loop - waiting for slave servers to finish");
		my $complete = "false";
		while($complete eq "false")
		{
			my @serverDoneFiles = ScanDirectory($serverDoneFileLocation);
			my @serverCount = split(',',$servers);
			my $doneFileCount = scalar @serverDoneFiles;
			my $serverCount = scalar @serverCount;
			if($doneFileCount == $serverCount) {
				$complete = "true";
			} else {
				addToLog("Waiting for slave servers to finish..sleeping");
				sleep(20);
			}
		}
		addToLog("Servers Finished Processing.");
		
		#limit lookupfile
		my $LUendDate = strftime('%Y%m%d',localtime);
		my $LUstartDate = strftime('%Y%m%d',localtime(time() - 86400*$lookupDays));
		my @lookupFiles = ScanDirectory($finalLUFileLocation,'.txt',$LUstartDate,$LUendDate);
			
		#Merge all lookups together - first load into hash to dedupe.
		my %hash = ();
		my $key;

		foreach(@lookupFiles)
		{
			addToLog("Processing file: $_");
			open (my $data,"<",$finalLUFileLocation.$_) or die $!;
			while (my $line =<$data>)
			{
				chomp ($line);
				my($key,$value) = split("\t", $line);
				$hash{$key} = [$value];
			}
		}
		
		open LOOKUP,'>',$finalLUCombinedLocation."masterLookup.txt" or die $!;
		while ( my ($key, $value) = each(%hash) ) {
			print LOOKUP "$key\t@{$value}\n";
		}
		close LOOKUP;
		addToLog("Finished writing master lookup file to $finalLUCombinedLocation limited from $LUstartDate to $LUendDate");
		
		#check to see if file needs splitting for DWB
		my @LUfilestat = stat $finalLUCombinedLocation."masterLookup.txt";
		my $LUsize = $LUfilestat[7];
		if($LUsize > 1200000000)
		{
			addToLog("Splitting Lookup File - it's $LUsize");
			splitLookup($finalLUCombinedLocation."masterLookup.txt");
		} else {
			addToLog("Not Splitting Lookup File - it's $LUsize");
		}
		
		#clean servers after
		if($cleanServersAfter eq "true")
		{
			addToLog("Cleaning up servers");
			foreach(@serverCount)
			{
				next if $_ eq "";
				my $string = $_;
				$string =~ /(.*):(.*)>(.*)/;
				my $server = $1;
				`RMDIR \\\\$server\\e\$\\Scripts\\UCPSlave\\ /S /Q`;
				addToLog("Removing directory on $server");
			}
		}
	}
addToLog("Master Script Terminated");
exit();
}

if($serverType eq "Slave")
{
	$logDirectory = "E:\\Scripts\\UCPSlave\\";
	open STDOUT, '>', "$logDirectory\\STDOUT.log";
	unlink($logDirectory."Log.txt");
	addToLog("Server Type: $serverType | Start Date: $startDate | End Date: $endDate");

	#setup varibles to be used in the cleaning / looping of the file.
	my $max = max @columnsPassed;
	my $columns = scalar(@columnsPassed);
	
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
		
		addToLog ("Begin Unzipping File: ".$_." Size: $filesize ");
		gzipFile($stagingDirectory.$_,$dcPhase1Directory.$_);
		$filesize = -s $dcPhase1Directory.$_.".txt";
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
		addToLog ("Begin Processing File: $dcPhase1FileName");
		my $start_run = time();
		if ($cleanData eq "false")
		{
			addToLog ("Not cleaning file $filename");
			addToLog ("Begin looping through file: $filename to extract ID's");
			while (<INFILE>) {
				chomp;
				my @f = split("\t",$_,$idFieldNum+1);
				#This section outputs the SCID and CIN to hash-map.
				if($f[$idFieldNum]!=""){
					$SCID_Evar_Mapping{"$f[8]-$f[9]"} = $f[$idFieldNum];
				}	
			}	
			close (INFILE);
			move($dcPhase1Directory.$filename.".txt", $dcOutputDirectory.$filename.".txt");
		} else {
			while (<INFILE>) {
				chomp;
				my @f = split("\t",$_,$max);

				#This bit of logic decides if the column should be printed
				#Only progress if this part is true, needs to be part of a modular function
				if($f[0] == 1 || ($f[0] == 2 && $f[203])){ 		
					#Columns should be the number of columns in the output
					for(my $i = 0; $i < $columns; $i ++ ){
						print DC_OUTFILE "$f[$columnsPassed[$i]]\t";
					}
					print DC_OUTFILE "\n"; #Finish up the line
				}
				
				#This section outputs the SCID to Evar53 to a hash-map.
				if($f[$idFieldNum]!=""){
					$SCID_Evar_Mapping{"$f[8]-$f[9]"} = $f[$idFieldNum];
					#Add pair to hash-map
				}	
			}	
			close (INFILE);
			#After complete delete file
			addToLog ("Finished Cleaning File: $dcPhase1FileName");
		}
		my $end_run = time();
		my $run_time = $end_run - $start_run;
		addToLog("$filename took $run_time seconds to complete\n");
		
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
	addToLog ("Removing DC File: $dcPhase1FileName");
	unlink($luPhase1FileName);
	addToLog ("Removing LU File: $luPhase1FileName");
	unlink($zipPhase1FileName);
	addToLog ("Removing ZIP File: $zipPhase1FileName");
	unlink($dcOutputFileName);
	addToLog ("Removing DC Output File: $zipPhase1FileName");
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
    my @return;

    foreach my $name (@names){
	   # use start and end date filtering if present.
	   my $fileDate = $name; 
	   $fileDate =~ /(\d{8})/;
	   $fileDate = $1;
	   if($startDate != "" && $endDate != "") {
	   next unless($fileDate >= $startDate && $fileDate <= $endDate)
	   }
	   
	   # Use a regular expression to ignore files beginning with a period
       next if ($name eq "");
	   next if ($name =~ m/^\./);
       next unless $name =~ /(.*$extension$)/;
		  push(@return, $name);
    }
	@return = sort @return;
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
				addToLog ("File already exists: $_");
			} else {
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

sub splitLookup
{
	my $file = shift();
	my $outputname = $file;

	die "Gimme file to open!\n" if !$file;
	die "Can't open INPUT file!\n" if !-r $file;

	my @filestat = stat $file;
	my $size = $filestat[7];
	my $parts = int($size / 1200000000)+1;
	my $chunk = int($size/$parts) + 1;

	open(FILE, $file);
	use bytes; # This statement makes "length" return bytes instead of characters
	my $i = 1;
	my $sub_file = '';
	while (<FILE>)
	  {
	  $sub_file .= $_;
	  if (length($sub_file) > $chunk)
		{
			open(OUTFILE, ">" . $outputname . ".split" . $i);
			print OUTFILE $sub_file;
			close OUTFILE;
			$sub_file = '';
			$i++;
		}
	  }

	open(OUTFILE, ">" . $outputname . ".split" . $i);
	print OUTFILE $sub_file;
	close OUTFILE;
	no bytes;
	close FILE;
}

sub GetSetting
{
my ($cfg_value,$cfg_filename,$cfg_default)=@_;
my $cfg_line;
open(CFGFILE,"<$cfg_filename") or warn "Can't open configuration file $cfg_filename.";
my @cf=<CFGFILE>;
foreach $cfg_line (@cf)
{
if (index($cfg_line,"#")==0) { next; } # Lines starting with a hash mark are comments
my @ln=split("=",$cfg_line);
if ($ln[0] =~ /$cfg_value/i) {
chomp $ln[1];
return $ln[1];
}
}
close CFGFILE;
return $cfg_default; # Return default if we can't find the value 
}