package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/fsnotify/fsnotify"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
)

var (
	mainDir     string
	tempDir     string
	sqlUser     string
	sqlPassword string
	sqlHost     string
	sqlPort     string
	sqlDatabase string
)

func init() {
	flag.StringVar(&mainDir, "mainDir", "./", "Main working directory")
	flag.StringVar(&tempDir, "tempDir", "/tmp", "Temporary directory for processing files")
	flag.StringVar(&sqlUser, "sqlUser", "root", "SQL database username")
	flag.StringVar(&sqlPassword, "sqlPassword", "", "SQL database password")
	flag.StringVar(&sqlHost, "sqlHost", "localhost", "SQL database host")
	flag.StringVar(&sqlPort, "sqlPort", "3306", "SQL database port")
	flag.StringVar(&sqlDatabase, "sqlDatabase", "mydb", "SQL database name")
	flag.Parse()
}

func main() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer func(watcher *fsnotify.Watcher) {
		err := watcher.Close()
		if err != nil {

		}
	}(watcher)

	done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					log.Println("New file detected:", event.Name)
					go processFile(event.Name) // process in a new goroutine
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("Error:", err)
			}
		}
	}()
	err = watcher.Add(mainDir)
	if err != nil {
		log.Fatal(err)
	}
	<-done
}

func processFile(filePath string) {
	fileName := filepath.Base(filePath)
	tempFilePath := filepath.Join(tempDir, fileName)

	// Move the file to the temp directory
	err := os.Rename(filePath, tempFilePath)
	if err != nil {
		log.Println("Error moving file:", err)
		return
	}

	// Check if file is a .gz archive and extract
	if filepath.Ext(fileName) == ".gz" {
		err = extractGzip(tempFilePath, tempDir)
		if err != nil {
			log.Println("Error extracting .gz file:", err)
			return
		}
		tempFilePath = tempFilePath[:len(tempFilePath)-len(".gz")]
	}

	// Process the file
	lines, err := processLines(tempFilePath)
	if err != nil {
		log.Println("Error processing file:", err)
	}

	// Example: Print each line or process further
	for _, line := range lines {
		fmt.Println(line)
	}
}
func extractGzip(src, destDir string) error {
	gzFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func(gzFile *os.File) {
		err := gzFile.Close()
		if err != nil {

		}
	}(gzFile)

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		return err
	}
	defer func(gzReader *gzip.Reader) {
		err := gzReader.Close()
		if err != nil {

		}
	}(gzReader)

	extractedFilePath := src[:len(src)-len(".gz")]
	extractedFile, err := os.Create(extractedFilePath)
	if err != nil {
		return err
	}
	defer func(extractedFile *os.File) {
		err := extractedFile.Close()
		if err != nil {

		}
	}(extractedFile)

	_, err = io.Copy(extractedFile, gzReader)
	return err
}

var regexLine = `(?P<Timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\+0000),(?P<Type>.*?),SessionId='(?P<SessionId>.*?)',LegId='(?P<LegId>.*?)',StartTime='(?P<StartTime>\d+)',ConnectedTime='(?P<ConnectedTime>\d+)',EndTime='(?P<EndTime>\d+)',FreedTime='(?P<FreedTime>\d+)',Duration='(?P<Duration>\d+)',TerminationCause='(?P<TerminationCause>.*?)',TerminationSource='(?P<TerminationSource>.*?)',Calling='(?P<Calling>\+?\d+)',Called='(?P<Called>\+?\d+|)',NAP='(?P<NAP>.*?)',Direction='(?P<Direction>.*?)',Media='(?P<Media>.*?)',Rtp:Rx='(?P<RtpRx>.*?)',Rtp:Tx='(?P<RtpTx>.*?)',T38:Rx='(?P<T38Rx>.*?)',T38:Tx='(?P<T38Tx>.*?)',Error:FromNetwork='(?P<ErrorFromNetwork>.*?)',Error:ToNetwork='(?P<ErrorToNetwork>.*?)',MOS='(?P<MOS>.*?)','-',NetworkQuality='(?P<NetworkQuality>.*?)','-'`

func processLines(filePath string) ([]CDR, error) {
	var cdrs []CDR

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	// Create a scanner to read the file
	scanner := bufio.NewScanner(file)

	// Example regex pattern
	r := regexp.MustCompile(regexLine)

	// Read file line by line
	for scanner.Scan() {
		line := scanner.Text()

		// Process each line
		match := r.FindStringSubmatch(line)
		if match == nil {
			log.Printf("Line did not match. Skipping line: %s\n", line)
			continue
		}

		// Construct CDR object from regex groups
		cdr := CDR{
			Timestamp:         match[r.SubexpIndex("Timestamp")],
			Type:              match[r.SubexpIndex("Type")],
			SessionID:         match[r.SubexpIndex("SessionId")],
			LegID:             match[r.SubexpIndex("LegId")],
			StartTime:         match[r.SubexpIndex("StartTime")],
			ConnectedTime:     match[r.SubexpIndex("ConnectedTime")],
			EndTime:           match[r.SubexpIndex("EndTime")],
			FreedTime:         match[r.SubexpIndex("FreedTime")],
			Duration:          match[r.SubexpIndex("Duration")],
			TerminationCause:  match[r.SubexpIndex("TerminationCause")],
			TerminationSource: match[r.SubexpIndex("TerminationSource")],
			Calling:           match[r.SubexpIndex("Calling")],
			Called:            match[r.SubexpIndex("Called")],
			NAP:               match[r.SubexpIndex("NAP")],
			Direction:         match[r.SubexpIndex("Direction")],
			Media:             match[r.SubexpIndex("Media")],
			RtpRx:             match[r.SubexpIndex("RtpRx")],
			RtpTx:             match[r.SubexpIndex("RtpTx")],
			T38Rx:             match[r.SubexpIndex("T38Rx")],
			T38Tx:             match[r.SubexpIndex("T38Tx")],
			ErrorFromNetwork:  match[r.SubexpIndex("ErrorFromNetwork")],
			ErrorToNetwork:    match[r.SubexpIndex("ErrorToNetwork")],
			MOS:               match[r.SubexpIndex("MOS")],
			NetworkQuality:    match[r.SubexpIndex("NetworkQuality")],
		}

		cdrs = append(cdrs, cdr)
	}

	// Check for any errors encountered while reading the file
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	return cdrs, nil
}
