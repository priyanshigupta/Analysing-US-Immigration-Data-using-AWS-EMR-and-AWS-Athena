# Analysing-US-Immigration-Data-using-AWS-EMR-and-AWS-Athena

## Data Model

<img src="Images/Untitled Diagram.jpg" alt="drawing" width="700" height="800"/>

## Scope the Project and Gather Data

Following are the datasets used for this project: 

#### Immigration Data
“Form I-94, the Arrival-Departure Record Card, is a form used by the U.S. Customs and BorderProtection (CBP) intended to keep track of the arrival and departure to/from the United States ofpeople who are not United States citizens or lawful permanent residents (with the exception of thosewho are entering using the Visa Waiver Program or Compact of Free Association, using BorderCrossing Cards, re-entering via automatic visa revalidation, or entering temporarily as crewmembers)” (
https://en.wikipedia.org/wiki/Form_I-94
) .It lists the traveler’s immigration category, portof entry, data of entry into the United States, status expiration date and had a unique 11-digitidentifying number assigned to it. Its purpose was to record the traveler’s lawful admission to theUnited States (
https://i94.cbp.dhs.gov/I94/(
This is the main dataset and there is a file for each month of the year of 2016 available in thedirectory ../../data/18-83510-I94-Data-2016/ . It is in SAS binary database storage format sas7bdat.This project uses the parquet files available in the workspace and the folder called sap_data. Thedata is for the month of the month of April of 2016 which has more than three million records(3.096.313). The fact table is derived from this table.

#### Airports Data
“Airport data includes IATA airport code.An IATA airport code, also known as an IATA locationidentifier, IATA station code or simply a location identifier, is a three-letter geocode designatingmany airports and metropolitan areas around the world, defined by the International Air TransportAssociation (IATA). IATA code is used in passenger reservation, ticketing and baggage-handlingsystems (
https://en.wikipedia.org/wiki/IATA_airport_code)”
. It was downloaded from a public domainsource (
http://ourairports.com/data/
)

#### U.S. City Demographic Data
This dataset contains information about the demographics of all US cities and census-designatedplaces with a population greater or equal to 65,000. This data comes from the US Census Bureau’s2015 American Community Survey. This product uses the Census Bureau Data API but is notendorsed or certified by the Census Bureau. The US City Demographics is the source of the STATEdimension in the data model and grouped by State.


* This data was transfered to the S3 bucket to make dimensional modelling easy.

## Explore and Assess the Data

1) i94 data had missing values, these columns were dropped.
2) Demographic dataset contains data for 48 states and doesnot have many missing values.
3) Most of the iata_code are missing but using it, we can get airports information of immigrants.
4) Drooped duplicate values from all dimensions as well as facts.
5) Grouped the data to provide an aggregated statistics per US state.
6) Renamed the columns of the dataset to more meaning full names.

## Define the Data Model

Used Star schema to do the dimensional modeling for this project. I-94 data was used as Fact table which was connecting to other dimensions by refering to their primary key.
This choice med it easy for my to analyse and gather more information pertaining to th ei94 data.

### Data Model

<img src="Images/Untitled Diagram.jpg" alt="drawing" width="700" height="800"/>

## Mapping Out Data Pipelines



