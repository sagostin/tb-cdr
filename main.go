package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"flag"
	"fmt"
	"github.com/fsnotify/fsnotify"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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
	// Initialize command-line flags
	flag.StringVar(&mainDir, "mainDir", "./new", "Main working directory")
	flag.StringVar(&tempDir, "tempDir", "./tmp", "Temporary directory for processing files")
	flag.StringVar(&sqlUser, "sqlUser", "root", "SQL database username")
	flag.StringVar(&sqlPassword, "sqlPassword", "", "SQL database password")
	flag.StringVar(&sqlHost, "sqlHost", "localhost", "SQL database host")
	flag.StringVar(&sqlPort, "sqlPort", "3306", "SQL database port")
	flag.StringVar(&sqlDatabase, "sqlDatabase", "mydb", "SQL database name")
	flag.Parse()
}

func main() {
	// Set up a new file watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Error(err)
	}
	defer func(watcher *fsnotify.Watcher) {
		err := watcher.Close()
		if err != nil {
			log.Error(err)
		}
	}(watcher)

	// Establish a connection to the SQL database
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", sqlUser, sqlPassword, sqlHost, sqlPort, sqlDatabase))
	if err != nil {
		log.Error(err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Error(
				err)
		}
	}(db)

	processExistingFiles(mainDir, db)
	processExistingFiles(tempDir, db)

	// Start watching the main directory for changes
	err = watcher.Add(mainDir)
	if err != nil {
		log.Error(err)
	}

	// Listen for events and errors
	done := make(chan bool)
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					srcFilePath := event.Name
					destFilePath := filepath.Join(tempDir, filepath.Base(event.Name))

					// Move the file to the temp directory
					err := os.Rename(srcFilePath, destFilePath)
					if err != nil {
						log.Println("Error moving file:", err)
					}

					log.Println("New file detected:", event.Name)
					go processFile(event.Name, db) // Process in a new goroutine
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("Error:", err)
			}
		}
	}()

	<-done
}

func processFile(filePath string, db *sql.DB) {
	// Get the file name and construct the temp file path
	fileName := filepath.Base(filePath)
	tempFilePath := filepath.Join(tempDir, fileName)

	// If the file is a .gz file, extract it
	if filepath.Ext(tempFilePath) == ".gz" {
		err := extractGzip(tempFilePath, tempDir)
		if err != nil {
			log.Println("Error extracting .gz file:", err)
			return
		}
		// Update the tempFilePath to the extracted file
		tempFilePath = tempFilePath[:len(tempFilePath)-len(filepath.Ext(tempFilePath))]
	}

	// Process the file if it's a .log file
	if filepath.Ext(tempFilePath) == ".log" {
		cdrs, err := processLines(tempFilePath)
		if err != nil {
			log.Println("Error processing log file:", err)
			return
		}

		// Remove the processed .log file
		err = os.Remove(tempFilePath)
		if err != nil {
			log.Println("Error removing processed file:", err)
			// Continue to database insertion even if file removal fails
		}

		// Insert CDR records into the database
		// Use a prepared SQL statement for database insertion
		// Prepare the SQL statement for inserting CDR records
		stmt, err := db.Prepare(`
    INSERT INTO tb_cdr 
    (Timestamp, Type, SessionID, LegID, StartTime, ConnectedTime, EndTime, FreedTime, Duration, 
    TerminationCause, TerminationSource, Calling, Called, NAP, Direction, Media, RtpRx, RtpTx, 
    T38Rx, T38Tx, ErrorFromNetwork, ErrorToNetwork, MOS, NetworkQuality) 
    VALUES 
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			log.Error("Error preparing insert statement:", err)
		}

		// Iterate over the CDR records and insert them into the database
		for _, cdr := range cdrs {
			_, err = stmt.Exec(
				cdr.Timestamp, cdr.Type, cdr.SessionID, cdr.LegID, cdr.StartTime, cdr.ConnectedTime, cdr.EndTime,
				cdr.FreedTime, cdr.Duration, cdr.TerminationCause, cdr.TerminationSource, cdr.Calling, cdr.Called,
				cdr.NAP, cdr.Direction, cdr.Media, cdr.RtpRx, cdr.RtpTx, cdr.T38Rx, cdr.T38Tx, cdr.ErrorFromNetwork,
				cdr.ErrorToNetwork, cdr.MOS, cdr.NetworkQuality,
			)
			if err != nil {
				log.Println("Error inserting CDR record:", err)
			}
		}

		if err != nil {
			log.Println("Error inserting CDR record into database:", err)
		}
	}
}

func processExistingFiles(directory string, db *sql.DB) {
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		log.Error("Error reading directory:", directory, err)
		return
	}
	for _, file := range files {
		filePath := filepath.Join(directory, file.Name())

		// Move file to tempDir if it's in mainDir
		if directory == mainDir {
			tempFilePath := filepath.Join(tempDir, file.Name())
			err := os.Rename(filePath, tempFilePath)
			if err != nil {
				log.Println("Error moving file:", err)
				continue // Skip to the next file
			}
			filePath = tempFilePath
		}

		go processFile(filePath, db)
	}
}

func extractGzip(src, destDir string) error {
	gzFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("error opening .gz file: %v", err)
	}
	defer func(gzFile *os.File) {
		err := gzFile.Close()
		if err != nil {

		}
	}(gzFile)

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		return fmt.Errorf("error creating gzip reader: %v", err)
	}
	defer func(gzReader *gzip.Reader) {
		err := gzReader.Close()
		if err != nil {

		}
	}(gzReader)

	// The name of the extracted file will be the same as the .gz file but without the .gz extension
	extractedFilePath := filepath.Join(destDir, strings.TrimSuffix(filepath.Base(src), ".gz"))
	extractedFile, err := os.Create(extractedFilePath)
	if err != nil {
		return fmt.Errorf("error creating extracted file: %v", err)
	}
	defer func(extractedFile *os.File) {
		err := extractedFile.Close()
		if err != nil {

		}
	}(extractedFile)

	_, err = io.Copy(extractedFile, gzReader)
	if err != nil {
		return fmt.Errorf("error writing extracted data: %v", err)
	}

	return nil
}

// CDR is a struct that contains the information from a CDR record

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
			log.Error(err)
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
