package omnik

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Storage provides an abstraction for the storage backend.
type Storage interface {
	// Insert inserts a sample into the storage backend.
	Insert(sample Sample)
	// HasMonth determines if data for a given month exists.
	HasMonth(m time.Time) bool
	// GetSamplesInRange retrieves all samples in the specified range.
	GetSamplesInRange(start time.Time, end time.Time) ([]Sample, error)
	// GetDailyKWHInRange retrieves the daily KWH values in the specified range.
	GetDailyKWHInRange(start time.Time, end time.Time) ([]TimeKWH, error)
	// GetLatestSample retrieves the last sample stored.
	GetLatestSample() (Sample, error)
	// GetSamplesForDate retrieves all samples on a given date.
	GetSamplesForDate(d time.Time) ([]Sample, error)
	// GetAdjecentDate retrieves the date adjecent to the given time.
	GetAdjecentDate(t time.Time, a Adjecency) (time.Time, error)
}

// SQL provides an SQL implementation of the storage backend.
type SQL struct {
	Storage
	initialized     bool
	Database        string
	db              *sql.DB
	insertStatement *sql.Stmt
}

func (s *SQL) initialize() {
	conn, err := sql.Open("mysql", s.Database)
	panicOnError(err)
	s.db = conn
	s.prepareTables()
	s.initializeInsertStatement()
	s.initialized = true

	go s.keepAlive()	
}

func (s *SQL) keepAlive() {
	ticks := time.NewTicker(time.Second * 30)
	for {
		s.db.Ping()
		<-ticks.C
	}
}

func (s *SQL) initializeInsertStatement() {
	stmt, err := s.db.Prepare(`INSERT samples SET 
	time_of_measurement=?,
	date=?,
	time=?,
	temp=?,
	total_kwh=?,
	today_kwh=?,
	total_hours=?,
	current_watts=?,
	pv_input_voltage_1=?,
	pv_input_voltage_2=?,
	pv_input_voltage_3=?,
	pv_input_amps_1=?,
	pv_input_amps_2=?,
	pv_input_amps_3=?,
	ac_output_voltage_1=?,
	ac_output_voltage_2=?,
	ac_output_voltage_3=?,
	ac_output_amps_1=?,
	ac_output_amps_2=?,
	ac_output_amps_3=?,
	ac_output_frequency_1=?,
	ac_output_frequency_2=?,
	ac_output_frequency_3=?,
	ac_output_watts_1=?,
	ac_output_watts_2=?,
	ac_output_watts_3=?
	`)
	panicOnError(err)
	s.insertStatement = stmt
}

func (s *SQL) ensureInitialized() {
	if !s.initialized {
		s.initialize()
	}
}

// Insert inserts a sample into the SQL database.
func (s *SQL) Insert(sample Sample) {
	s.ensureInitialized()
	_, err := s.insertStatement.Exec(sample.Timestamp, sample.Date, sample.Time, sample.Temperature, sample.EnergyTotal, sample.EnergyToday, sample.EnergyHours, sample.Power, sample.PvVoltage1, sample.PvVoltage2, sample.PvVoltage3, sample.PvCurrent1, sample.PvCurrent2, sample.PvCurrent3, sample.ACVoltage1, sample.ACVoltage2, sample.ACVoltage3, sample.ACCurrent1, sample.ACCurrent2, sample.ACCurrent3, sample.ACFrequency1, sample.ACFrequency2, sample.ACFrequency3, sample.ACPower1, sample.ACPower2, sample.ACPower3)
	if err != nil {
		fmt.Println("Insert error:", err)
	}
}

func (s *SQL) prepareTables() {
	tables := []string{"samples"}

	for _, table := range tables {
		if !s.tableExists(table) {
			s.createTable(table)
		}
	}
}

func (s *SQL) tableExists(tableName string) bool {
	rows, err := s.db.Query("SHOW TABLES")
	panicOnError(err)

	for rows.Next() {
		var row string
		rows.Scan(&row)
		if row == tableName {
			return true
		}
	}
	return false
}

func (s *SQL) createTable(tableName string) {
	var query string

	switch tableName {
	case "samples":
		query = `CREATE TABLE samples (
			id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
			time_of_measurement DATETIME,
			date DATE,
			time TIME,
			temp FLOAT,
			total_kwh FLOAT,
			today_kwh FLOAT,
			total_hours INT,
			current_watts FLOAT,
			pv_input_voltage_1 FLOAT,
			pv_input_voltage_2 FLOAT,
			pv_input_voltage_3 FLOAT,
			pv_input_amps_1 FLOAT,
			pv_input_amps_2 FLOAT,
			pv_input_amps_3 FLOAT,
			ac_output_voltage_1 FLOAT,
			ac_output_voltage_2 FLOAT,
			ac_output_voltage_3 FLOAT,
			ac_output_amps_1 FLOAT,
			ac_output_amps_2 FLOAT,
			ac_output_amps_3 FLOAT,
			ac_output_frequency_1 FLOAT,
			ac_output_frequency_2 FLOAT,
			ac_output_frequency_3 FLOAT,
			ac_output_watts_1 FLOAT,
			ac_output_watts_2 FLOAT,
			ac_output_watts_3 FLOAT
			)`
	default:
		panic("Unknown table: " + tableName)
	}

	_, err := s.db.Exec(query)
	panicOnError(err)
}

// HasMonth determines if data exists in the database for a given month.
func (s *SQL) HasMonth(m time.Time) bool {
	s.ensureInitialized()
	r, err := s.db.Query("SELECT * FROM samples WHERE date LIKE ? LIMIT 1", m.Format("2006-01%"))

	if err != nil {
		return false
	}

	return r.Next()
}

// TimeKWH represents the total KWH generated on a given date.
type TimeKWH struct {
	Date time.Time
	KWH  float32
}

// GetSamplesInRange retrieves all samples in the specified range.
func (s *SQL) GetSamplesInRange(start time.Time, end time.Time) ([]Sample, error) {
	s.ensureInitialized()
	q := "SELECT * FROM samples WHERE date >= ? AND date <= ? ORDER BY date ASC;"
	
	r, err := s.db.Query(q, start.Format("2006-01-02"), end.Format("2006-01-02"))
	return s.queryResultToSampleArr(r, err)
}

func (s *SQL) queryResultToSampleArr(r *sql.Rows, err error) ([]Sample, error) {
	var data []Sample
	if err != nil {
		return data, err
	}

	for r.Next() {
		var id int
		sample := Sample{}
		r.Scan(&id, &sample.Timestamp, &sample.Date, &sample.Time, &sample.Temperature, &sample.EnergyTotal, &sample.EnergyToday, &sample.EnergyHours, &sample.Power, &sample.PvVoltage1, &sample.PvVoltage2, &sample.PvVoltage3, &sample.PvCurrent1, &sample.PvCurrent2, &sample.PvCurrent3, &sample.ACVoltage1, &sample.ACVoltage2, &sample.ACVoltage3, &sample.ACCurrent1, &sample.ACCurrent2, &sample.ACCurrent3, &sample.ACFrequency1, &sample.ACFrequency2, &sample.ACFrequency3, &sample.ACPower1, &sample.ACPower2, &sample.ACPower3)
		data = append(data, sample)
	}

	return data, nil
}

// GetDailyKWHInRange retrieves the daily KWH values in the specified range.
func (s *SQL) GetDailyKWHInRange(start time.Time, end time.Time) ([]TimeKWH, error) {
	s.ensureInitialized()
	var data []TimeKWH
	q := "SELECT date, max(today_kwh) FROM samples WHERE date >= ? AND date <= ? GROUP BY date;"
	r, err := s.db.Query(q, start.Format("2006-01-02"), end.Format("2006-01-02"))
	if err != nil {
		return data, err
	}

	for r.Next() {
		var rawdate string
		var kwh float32

		r.Scan(&rawdate, &kwh)

		date, _ := time.Parse("2006-01-02", rawdate)
		v := TimeKWH{
			Date: date,
			KWH:  kwh,
		}
		data = append(data, v)
	}

	return data, nil
}

// GetLatestSample retrieves the last sample stored.
func (s *SQL) GetLatestSample() (Sample, error) {
	s.ensureInitialized()
	sample := Sample{}
	query := `SELECT * FROM samples
		ORDER BY time_of_measurement DESC
		LIMIT 1`
	result, err := s.db.Query(query)
	if err != nil {
		return sample, err
	}

	defer result.Close()
	if !result.Next() {
		return sample, errors.New("No results")
	}

	var id int
	result.Scan(&id, &sample.Timestamp, &sample.Date, &sample.Time, &sample.Temperature, &sample.EnergyTotal, &sample.EnergyToday, &sample.EnergyHours, &sample.Power, &sample.PvVoltage1, &sample.PvVoltage2, &sample.PvVoltage3, &sample.PvCurrent1, &sample.PvCurrent2, &sample.PvCurrent3, &sample.ACVoltage1, &sample.ACVoltage2, &sample.ACVoltage3, &sample.ACCurrent1, &sample.ACCurrent2, &sample.ACCurrent3, &sample.ACFrequency1, &sample.ACFrequency2, &sample.ACFrequency3, &sample.ACPower1, &sample.ACPower2, &sample.ACPower3)
	return sample, nil
}

// GetSamplesForDate retrieves all samples on a given date.
func (s *SQL) GetSamplesForDate(d time.Time) ([]Sample, error) {
	s.ensureInitialized()
	q := "SELECT * FROM samples WHERE date =? ORDER BY time_of_measurement ASC"
	r, err := s.db.Query(q, d.Format("2006-01-02"))
	return s.queryResultToSampleArr(r, err)
}

// GetAdjecentDate retrieves the date adjecent to the given time.
func (s *SQL) GetAdjecentDate(t time.Time, a Adjecency) (time.Time, error) {
	s.ensureInitialized()
	var op, sort string
	switch a.(type) {
	case Before:
		op = "<"
		sort = "DESC"
	case After:
		op = ">"
		sort = "ASC"
	default:
		return time.Now(), fmt.Errorf("Unknown adjecency")
	}

	d := t.Format("2006-01-02")
	result, err := s.db.Query("SELECT DISTINCT date FROM samples WHERE date "+op+" ? ORDER BY date "+sort+" LIMIT 1", d)
	if err != nil {
		return time.Now(), err
	}

	defer result.Close()
	if !result.Next() {
		return time.Now(), fmt.Errorf("No previous date was found")
	}

	date := ""
	result.Scan(&date)

	return time.Parse("2006-01-02", date)
}
