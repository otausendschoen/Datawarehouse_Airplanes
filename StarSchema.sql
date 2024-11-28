CREATE TABLE Dim_Aircraft (
    aircraftID SERIAL PRIMARY KEY, -- Surrogate key for joining
    aircraftRegistration CHAR(6) NOT NULL UNIQUE, -- Unique registration code
    model VARCHAR(50),
    manufacturer VARCHAR(50)
);

CREATE TABLE Dim_Time (
    timeID SERIAL PRIMARY KEY, -- Surrogate key for joining
    day DATE NOT NULL UNIQUE, -- Date
    month INT NOT NULL,       -- Month (1–12)
    year INT NOT NULL         -- Year (e.g., 2024)
);

CREATE TABLE "bda-amos".Fact_FlightMetrics (
    factID SERIAL PRIMARY KEY,
    aircraftID INT NOT NULL REFERENCES Dim_Aircraft(aircraftID),
    timeID INT NOT NULL REFERENCES Dim_Time(timeID),
    
    -- Metrics
    totalFlightHours DECIMAL(10, 2),
    totalFlightCycles INT,
    delayRate DECIMAL(5, 2),
    cancellationRate DECIMAL(5, 2),
    aircraftDaysOutOfService INT
);

select * from "bda-amos".attachments a 


