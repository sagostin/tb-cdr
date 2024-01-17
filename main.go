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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var (
	mainDir     string
	tempDir     string
	archiveDir  string
	sqlUser     string
	sqlPassword string
	sqlHost     string
	sqlPort     string
	sqlDatabase string
	sqlDB       *sql.DB
)

func init() {
	// Initialize command-line flags
	flag.StringVar(&mainDir, "mainDir", "./new", "Main working directory")
	flag.StringVar(&tempDir, "tempDir", "./tmp", "Temporary directory for processing files")
	flag.StringVar(&archiveDir, "archiveDir", "./archive", "Archive directory for processed files")
	flag.StringVar(&sqlUser, "sqlUser", "root", "SQL database username")
	flag.StringVar(&sqlPassword, "sqlPassword", "", "SQL database password")
	flag.StringVar(&sqlHost, "sqlHost", "localhost", "SQL database host")
	flag.StringVar(&sqlPort, "sqlPort", "3306", "SQL database port")
	flag.StringVar(&sqlDatabase, "sqlDatabase", "mydb", "SQL database name")
	flag.Parse()
}

func initDB() (*sql.DB, error) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", sqlUser, sqlPassword, sqlHost, sqlPort, sqlDatabase))
	if err != nil {
		return nil, err
	}
	// Set up connection pooling here if needed
	return db, nil
}

func main() {
	log.Info("Starting CDR processor")

	// Initialize the database connection
	db, err := initDB()
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Error(err)
		}
	}(db)

	sqlDB = db

	log.Info("Database connection initialized")

	// Initialize the file watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf("Failed to initialize file watcher: %v", err)
	}
	defer func(watcher *fsnotify.Watcher) {
		err := watcher.Close()
		if err != nil {
			log.Error(err)
		}
	}(watcher)

	// Start the processing routines
	go watchDirectory(mainDir, watcher, true) // Watch for .gz files in mainDir
	//go watchDirectory(tempDir, watcher, false) // Watch for .log files in tempDir
	log.Info("Started processing routines")

	processExistingFiles(mainDir, tempDir)
	log.Info("Existing files processed")

	// Wait for termination signal
	select {}
}

// processExistingFiles processes all existing files in the directory
func processExistingFiles(mainDir string, tempDir string) {
	files, err := os.ReadDir(mainDir)
	if err != nil {
		log.Errorf("Error reading directory '%s': %v", mainDir, err)
		return
	}

	var wg sync.WaitGroup
	for _, fileInfo := range files {
		log.Warn("Processing file:", fileInfo.Name())

		wg.Add(1)
		go func(fileInfo os.DirEntry) {
			defer wg.Done()
			err := extractGzip(filepath.Join(mainDir, fileInfo.Name()), tempDir)
			if err != nil {
				log.Error(err)
			}
		}(fileInfo)
	}
	wg.Wait()
}

// watchDirectory sets up a watcher on the specified directory
func watchDirectory(directory string, watcher *fsnotify.Watcher, isMainDir bool) {
	err := watcher.Add(directory)

	if err != nil {
		log.Error(err)
	}

	for {
		select {
		case event, _ := <-watcher.Events:
			// ... [error handling]
			if isMainDir && event.Op&fsnotify.Write == fsnotify.Write && filepath.Ext(event.Name) == ".gz" {
				log.Warn("New .gz file:", event.Name)
				go handleNewGzFile(event.Name)
			} else if !isMainDir && event.Op&fsnotify.Write == fsnotify.Write && filepath.Ext(event.Name) == ".log" {
				log.Warn("New .log file:", event.Name)
				go func() {
					_, err := processLogFile(event.Name)
					if err != nil {
						log.Error(err)
					}
				}()
			}

		case err, _ := <-watcher.Errors:
			log.Error(err)
		}
	}
}

// handleNewGzFile handles new .gz files in the main directory
func handleNewGzFile(filePath string) {
	// Extract the .gz file to the tempDir
	err := extractGzip(filePath, tempDir)
	if err != nil {
		log.Errorf("Error extracting .gz file '%s': %v", filePath, err)
		return
	}
}

func dbPush(cdrs []CDR) error {
	// Get the file name and construct the temp file path

	// Insert CDR records into the database
	// Use a prepared SQL statement for database insertion
	// Prepare the SQL statement for inserting CDR records
	stmt, err := sqlDB.Prepare(`
    INSERT INTO tb_cdr 
    (Timestamp, Type, SessionID, LegID, StartTime, ConnectedTime, EndTime, FreedTime, Duration, 
    TerminationCause, TerminationSource, Calling, Called, NAP, Direction, Media, RtpRx, RtpTx, 
    T38Rx, T38Tx, ErrorFromNetwork, ErrorToNetwork, MOS, NetworkQuality) 
    VALUES 
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Error("Error preparing insert statement:", err)
		return err
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
			return err
		}
	}
	if err != nil {
		log.Println("Error inserting CDR record into database:", err)
		return err
	}

	return nil
}

/*func processFile(file string) {
	// Get the file name and construct the temp file path
	fileName := filepath.Base(file)
	tempFilePath := filepath.Join(tempDir, fileName)

	// Process the file if it's a .log file
	if filepath.Ext(tempFilePath) == ".log" {
		cdrs, err := processLogFile(tempFilePath)
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
		stmt, err := sqlDB.Prepare(`
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
}*/

func extractGzip(src, destDir string) error {
	gzFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("error opening .gz file: %v", err)
	}
	defer gzFile.Close()

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		return fmt.Errorf("error creating gzip reader: %v", err)
	}
	defer gzReader.Close()

	extractedFilePath := filepath.Join(destDir, strings.TrimSuffix(filepath.Base(src), ".gz"))
	extractedFile, err := os.Create(extractedFilePath)
	if err != nil {
		return fmt.Errorf("error creating extracted file: %v", err)
	}
	defer extractedFile.Close()

	_, err = io.Copy(extractedFile, gzReader)
	if err != nil {
		return fmt.Errorf("error writing extracted data: %v", err)
	}

	cdrs, err := processLogFile(extractedFilePath)

	// Process the extracted log file
	if err != nil {
		return fmt.Errorf("error processing log file: %v", err)
	}

	// Push CDR records to the database (assuming dbPush function exists)
	err = dbPush(cdrs)
	if err = dbPush(cdrs); err != nil {
		return fmt.Errorf("error pushing data to database: %v", err)
	}

	// Move the .gz file to the archive directory after processing
	archivePath := filepath.Join(archiveDir, filepath.Base(src))
	if err = os.Rename(src, archivePath); err != nil {
		return fmt.Errorf("error moving .gz file to archive: %v", err)
	}

	return nil
}

// CDR is a struct that contains the information from a CDR record

var (
	regexStart  = `(?P<Timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+-\d+),BEG,SessionId='(?P<SessionId>[^']+)',LegId='(?P<LegId>[^']+)',StartTime='(?P<StartTime>[^']+)',ConnectedTime='(?P<ConnectedTime>[^']+)',Calling='(?P<Calling>[^']+)',Called='(?P<Called>[^']+)',NAP='(?P<NAP>[^']+)',Protocol='(?P<Protocol>[^']+)',Direction='(?P<Direction>[^']+)'`
	regexUpdate = `^(?P<Timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+-\d+),UPD,SessionId='(?P<SessionId>[^']+)',LegId='(?P<LegId>[^']+)',Rtp:Rx='(?P<RtpRx>[^']+)',Rtp:Tx='(?P<RtpTx>[^']+)',T38:Rx='(?P<T38Rx>[^']+)',T38:Tx='(?P<T38Tx>[^']+)',Error:FromNetwork='(?P<ErrorFromNetwork>[^']+)',Error:ToNetwork='(?P<ErrorToNetwork>[^']+)'`
	regexEnd    = `(?P<Timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+-\d{4}),END,SessionId='(?P<SessionId>[^']+)',LegId='(?P<LegId>[^']+)',StartTime='(?P<StartTime>[^']+)',ConnectedTime='(?P<ConnectedTime>[^']+)',EndTime='(?P<EndTime>[^']+)',FreedTime='(?P<FreedTime>[^']+)',Duration='(?P<Duration>[^']+)',TerminationCause='(?P<TerminationCause>[^']+)',TerminationSource='(?P<TerminationSource>[^']+)',Calling='(?P<Calling>[^']+)',Called='(?P<Called>[^']+)',NAP='(?P<NAP>.*?)',Direction='(?P<Direction>.*?)',Media='(?P<Media>.*?)',Rtp:Rx='(?P<RtpRx>.*?)',Rtp:Tx='(?P<RtpTx>.*?)',T38:Rx='(?P<T38Rx>.*?)',T38:Tx='(?P<T38Tx>.*?)',Error:FromNetwork='(?P<ErrorFromNetwork>.*?)',Error:ToNetwork='(?P<ErrorToNetwork>.*?)',MOS=(?P<MOS>.*?),NetworkQuality=(?P<NetworkQuality>.*)`
)

func processLogFile(filePath string) ([]CDR, error) {
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

	// Read file line by line
	for scanner.Scan() {
		line := scanner.Text()
		var cdr CDR
		var match []string

		if strings.Contains(line, "BEG") {
			r := regexp.MustCompile(regexStart)
			match = r.FindStringSubmatch(line)
			if match != nil {
				cdr = CDR{
					Timestamp:     match[r.SubexpIndex("Timestamp")],
					Type:          "BEG",
					SessionID:     match[r.SubexpIndex("SessionId")],
					LegID:         match[r.SubexpIndex("LegId")],
					StartTime:     match[r.SubexpIndex("StartTime")],
					ConnectedTime: match[r.SubexpIndex("ConnectedTime")],
					Calling:       match[r.SubexpIndex("Calling")],
					Called:        match[r.SubexpIndex("Called")],
					NAP:           match[r.SubexpIndex("NAP")],
					Protocol:      match[r.SubexpIndex("Protocol")],
					Direction:     match[r.SubexpIndex("Direction")],
				}
			}
		} else if strings.Contains(line, "UPD") {
			r := regexp.MustCompile(regexUpdate)
			match = r.FindStringSubmatch(line)
			if match != nil {
				cdr = CDR{
					Timestamp:        match[r.SubexpIndex("Timestamp")],
					Type:             "UPD",
					SessionID:        match[r.SubexpIndex("SessionId")],
					LegID:            match[r.SubexpIndex("LegId")],
					RtpRx:            match[r.SubexpIndex("RtpRx")],
					RtpTx:            match[r.SubexpIndex("RtpTx")],
					T38Rx:            match[r.SubexpIndex("T38Rx")],
					T38Tx:            match[r.SubexpIndex("T38Tx")],
					ErrorFromNetwork: match[r.SubexpIndex("ErrorFromNetwork")],
					ErrorToNetwork:   match[r.SubexpIndex("ErrorToNetwork")],
				}
			}
		} else if strings.Contains(line, "END") {
			r := regexp.MustCompile(regexEnd)
			match = r.FindStringSubmatch(line)
			if match != nil {
				cdr = CDR{
					Timestamp:         match[r.SubexpIndex("Timestamp")],
					Type:              "END",
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
					MOS:               match[r.SubexpIndex("MOS")],            // Assuming MOS1 and MOS2 need to be combined
					NetworkQuality:    match[r.SubexpIndex("NetworkQuality")], // Assuming NetworkQuality1 and NetworkQuality2 need to be combined
				}
			}
		} else {
			log.Printf("Unknown CDR type. Skipping line: %s\n", line)
			continue
		}

		if match != nil {
			cdrs = append(cdrs, cdr)
		} else {
			log.Printf("Line did not match. Skipping line: %s\n", line)
		}
	}

	// Check for any errors encountered while reading the file
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	return cdrs, nil
}
