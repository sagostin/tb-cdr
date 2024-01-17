# CDR Processor Application

This Go application is designed to process Call Detail Records (CDR) from `.gz` files, extract information, and store it in a MySQL database.

## Features

* Watches a specified directory for new `.gz` files.
* Extracts `.gz` files to a temporary directory.
* Processes extracted `.log` files to parse CDR data.
* Inserts parsed data into a MySQL database.
* Moves processed `.gz` files to an archive directory.

## Setup and Installation

### Prerequisites

* Golang installed.
* MySQL server running.

### Steps to Run

1. **Clone the Repository:**

   ```shell
   git clone [repository-url]
   cd [repository-directory]
   ```
2. **Build the Application:**

   ```shell
   go build -o cdr_processor
   ```
3. **Run the Application:**

   ```shell
   ./cdr_processor
   ```

## Configuration

The application can be configured using command-line flags:

* `mainDir`: Main working directory for `.gz` files.
* `tempDir`: Temporary directory for processing files.
* `archiveDir`: Archive directory for processed files.
* `sqlUser`: SQL database username.
* `sqlPassword`: SQL database password.
* `sqlHost`: SQL database host.
* `sqlPort`: SQL database port.
* `sqlDatabase`: SQL database name.

Example:

```shell
./cdr_processor -mainDir="./new" -tempDir="./tmp" -archiveDir="./archive" -sqlUser="root" -sqlPassword="password" -sqlHost="localhost" -sqlPort="3306" -sqlDatabase="mydb"
```

## Database Setup

### Creating the Database and Table

Run the following SQL commands to create the necessary database and table:

```sql
USE tb_cdr;

CREATE TABLE IF NOT EXISTS tb_cdr (
Timestamp TIMESTAMP,
Type VARCHAR(255),
SessionID VARCHAR(255),
LegID VARCHAR(255),
StartTime VARCHAR(255),
ConnectedTime VARCHAR(255),
EndTime VARCHAR(255),
FreedTime VARCHAR(255),
Duration VARCHAR(255),
TerminationCause VARCHAR(255),
TerminationSource VARCHAR(255),
Calling VARCHAR(255),
Called VARCHAR(255),
NAP VARCHAR(255),
Direction VARCHAR(255),
Media VARCHAR(255),
RtpRx VARCHAR(255),
RtpTx VARCHAR(255),
T38Rx VARCHAR(255),
T38Tx VARCHAR(255),
ErrorFromNetwork VARCHAR(255),
ErrorToNetwork VARCHAR(255),
MOS VARCHAR(255),
NetworkQuality VARCHAR(255)
);
```


</code></div></div></pre>
