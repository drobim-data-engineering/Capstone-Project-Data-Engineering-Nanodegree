class SqlCreate:
    create_dim_schema = ("""CREATE SCHEMA IF NOT EXISTS DIM;""")
    create_staging_schema = ("""CREATE SCHEMA IF NOT EXISTS STAGING;""")
    create_fact_schema = ("""CREATE SCHEMA IF NOT EXISTS FACT;""")

    create_table_staging_covid_19 = ("""
    CREATE TABLE IF NOT EXISTS staging.us_covid_19 (
    UID                int
    ,iso2               varchar
    ,iso3               varchar
    ,code3              varchar
    ,FIPS               float
    ,Admin2             varchar
    ,Province_State     varchar
    ,Country_Region     varchar
    ,Lat                float
    ,Long               float
    ,Combined_Key       varchar
    ,Date               date
    ,Confirmed          int
    ,Deaths             int
    );
    """)

    create_table_staging_us_demographics = ("""
    CREATE TABLE IF NOT EXISTS staging.us_demographics (
     City                      varchar
    ,State                     varchar
    ,Median_Age                float
    ,Male_Population           float
    ,Female_Population         float
    ,Total_Population          int
    ,Number_of_Veterans        float
    ,Foreign_born              float
    ,Average_Household_Size    float
    ,State_Code                varchar
    ,Race                      varchar
    ,"Count"                   int
    );
    """)

    create_table_staging_us_accidents = ("""
    CREATE TABLE IF NOT EXISTS staging.us_accidents (
     ID                  varchar
    ,Source              varchar
    ,TMC                 float
    ,Severity            int
    ,Start_Time          timestamp
    ,End_Time            timestamp
    ,Start_Lat           float
    ,Start_Lng           float
    ,End_Lat             float
    ,End_Lng             float
    ,Distance            float
    ,Description         varchar(65535)
    ,Number              float
    ,Street              varchar
    ,Side                varchar
    ,City                varchar
    ,County              varchar
    ,State               varchar
    ,Zipcode             varchar
    ,Country             varchar
    ,Timezone            varchar
    ,Airport_Code        varchar
    ,Weather_Timestamp   timestamp
    ,Temperature         float
    ,Wind_Chill          float
    ,Humidity            float
    ,Pressure            float
    ,Visibility          float
    ,Wind_Direction      varchar
    ,Wind_Speed          float
    ,Precipitation       float
    ,Weather_Condition   varchar
    ,Amenity             boolean
    ,Bump                boolean
    ,Crossing            boolean
    ,Give_Way            boolean
    ,Junction            boolean
    ,No_Exit             boolean
    ,Railway             boolean
    ,Roundabout          boolean
    ,Station             boolean
    ,Stop                boolean
    ,Traffic_Calming     boolean
    ,Traffic_Signal      boolean
    ,Turning_Loop        boolean
    ,Sunrise_Sunset      varchar
    ,Civil_Twilight      varchar
    ,Nautical_Twilight   varchar
    ,Astronomical_Twilight varchar
    );
    """)

    create_table_dim_dates = ("""
    CREATE TABLE IF NOT EXISTS dim.dates (
     date_id              Int PRIMARY KEY
    ,full_date            Date NOT NULL
    ,year_number          SMALLINT NOT NULL
    ,year_week_number     SMALLINT NOT NULL
    ,year_day_number      SMALLINT NOT NULL
    ,qtr_number           SMALLINT NOT NULL
    ,month_number         SMALLINT NOT NULL
    ,month_name           CHAR(9)  NOT NULL
    ,month_day_number     SMALLINT NOT NULL
    ,week_day_number      SMALLINT NOT NULL
    ,day_name             CHAR(9)  NOT NULL
    ,day_is_weekday       SMALLINT NOT NULL
    ,day_is_last_of_month SMALLINT NOT NULL
    ) DISTSTYLE ALL SORTKEY (date_id);
    """)

    create_table_dim_accident_address = ("""
    CREATE TABLE IF NOT EXISTS dim.accident_address (
    ACCIDENT_ADDRESS_ID  varchar(32) PRIMARY KEY
    ,Number              float
    ,Street              varchar
    ,Side                varchar
    ,State               varchar
    ,City                varchar
    ,County              varchar
    ,Zipcode             varchar
    ,Country             varchar
    );
    """)

    create_table_dim_accident_location_details = ("""
    CREATE TABLE IF NOT EXISTS dim.accident_location_details (
    ACCIDENT_LOCATION_DETAILS_ID varchar(32) PRIMARY KEY
    ,Amenity             boolean
    ,Bump                boolean
    ,Crossing            boolean
    ,Give_Way            boolean
    ,Junction            boolean
    ,No_Exit             boolean
    ,Railway             boolean
    ,Roundabout          boolean
    ,Station             boolean
    ,Stop                boolean
    ,Traffic_Calming     boolean
    ,Traffic_Signal      boolean
    ,Turning_Loop        boolean
    );
    """)

    create_table_dim_weather_condition = ("""
    CREATE TABLE IF NOT EXISTS dim.weather_condition (
    Weather_Condition_ID varchar(32) PRIMARY KEY
    ,Weather_Timestamp   timestamp
    ,Temperature         float
    ,Wind_Chill          float
    ,Humidity            float
    ,Pressure            float
    ,Visibility          float
    ,Wind_Direction      varchar
    ,Wind_Speed          float
    ,Precipitation       float
    ,Weather_Condition   varchar
    );
    """)

    create_table_fact_us_demographics = ("""
    CREATE TABLE IF NOT EXISTS fact.us_demographics (
    City_ID                    varchar PRIMARY KEY
    ,City                      varchar
    ,State                     varchar
    ,State_Code                varchar
    ,Median_Age                float
    ,Male_Population           float
    ,Female_Population         float
    ,Total_Population          int
    ,Number_of_Veterans        float
    ,Foreign_born              float
    ,Average_Household_Size    float
    );
    """)

    create_table_fact_us_accidents = ("""
    CREATE TABLE IF NOT EXISTS fact.us_accidents (
     Accident_ID                  varchar PRIMARY KEY
    ,Description                  varchar(65535)
    ,Severity                     int
    ,Start_Time                   timestamp
    ,End_Time                     timestamp
    ,Timezone                     varchar
    ,Start_Lat                    float
    ,Start_Lng                    float
    ,Source                       varchar
    ,City_ID                      varchar
    ,ACCIDENT_ADDRESS_ID          varchar
    ,ACCIDENT_LOCATION_DETAILS_ID varchar
    ,Weather_Condition_ID         varchar
    ,Date_ID                      int
    );
    """)

    create_table_fact_covid_19 = ("""
    CREATE TABLE IF NOT EXISTS fact.us_covid_19 (
     Covid_ID   INT PRIMARY KEY
    ,State      varchar
    ,County     varchar
    ,Date       date
    ,Confirmed  int
    ,Deaths     int
    ,Date_ID    int
    );
    """)