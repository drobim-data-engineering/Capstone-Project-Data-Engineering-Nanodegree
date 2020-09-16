class SqlLoad:
    insert_acc_location_details_table = ("""
    Select Distinct md5(Start_Lat::varchar(256) || Start_Lng::varchar(256))::varchar(32) as ACCIDENT_LOCATION_DETAILS_ID
    ,Amenity
    ,Bump
    ,Crossing
    ,Give_Way
    ,Junction
    ,No_Exit
    ,Railway
    ,Roundabout
    ,Station
    ,Stop
    ,Traffic_Calming
    ,Traffic_Signal
    ,Turning_Loop
    From staging.us_accidents
    Where Coalesce(Start_Lat,'') <> '' And Coalesce(Start_Lng,'') <> '';
    """)

    insert_wth_condition_table = ("""
    Select Distinct extract('epoch' from Weather_Timestamp)::varchar || Airport_Code As Weather_Condition_ID
    ,Weather_Timestamp
    ,Temperature
    ,Wind_Chill
    ,Humidity
    ,Pressure
    ,Visibility
    ,Wind_Direction
    ,Wind_Speed
    ,Precipitation
    ,Weather_Condition
    From staging.us_accidents
    Where Weather_Timestamp Is Not Null And Coalesce(Airport_Code,'') <> '';
    """)

    insert_acc_address_table = ("""
    Select Distinct md5(Coalesce(Number,'')::varchar(256)
                    || Coalesce(Street,'')::varchar(256)
                    || Coalesce(Side,'')::varchar(256)
                    || Coalesce(City,'')::varchar(256)
                    || Coalesce(County,'')::varchar(256)
                    || Coalesce(State,'')::varchar(256)
                    || Coalesce(Zipcode,'')::varchar(256)
                    || Coalesce(Country,'')::varchar(256))::varchar(32) As ACCIDENT_ADDRESS_ID
    ,Number::Int As Number
    ,Street
    ,Side
    ,State
    ,City
    ,County
    ,Zipcode
    ,Country
    From staging.us_accidents;
    """)

    insert_dates_table = ("""
    SELECT extract('epoch' from date)                                 AS date_id
        ,date                                                         AS full_date
        ,cast(extract(YEAR FROM date) AS SMALLINT)                    AS year_number
        ,cast(extract(WEEK FROM date) AS SMALLINT)                    AS year_week_number
        ,cast(extract(DOY FROM date) AS SMALLINT)                     AS year_day_number
        ,cast(to_char(date, 'Q') AS SMALLINT)                         AS qtr_number
        ,cast(extract(MONTH FROM date) AS SMALLINT)                   AS month_number
        ,to_char(date, 'Month')                                       AS month_name
        ,cast(extract(DAY FROM date) AS SMALLINT)                     AS month_day_number
        ,cast(to_char(date, 'D') AS SMALLINT)                         AS week_day_number
        ,to_char(date, 'Day')                                         AS day_name
        ,CASE WHEN to_char(date, 'D') IN ('1', '7') THEN 0 ELSE 1 END AS day_is_weekday
        ,CASE WHEN extract(DAY FROM (date + (1 - extract(DAY FROM date)) :: INTEGER + INTERVAL '1' MONTH) :: DATE - INTERVAL '1' DAY) = extract(DAY FROM date)  THEN 1
            ELSE 0 END AS day_is_last_of_month
    FROM (
            Select ('2015-12-31'::date + row_number() over (order by true))::date as date
            From staging.us_accidents
            Limit 3650
        );
    """)

    insert_demographics_table = ("""
    Select Distinct md5(City || State_Code)::varchar(32) City_ID
    ,City
    ,State
    ,State_Code
    ,Median_Age
    ,Male_Population
    ,Female_Population
    ,Total_Population
    ,Number_of_Veterans
    ,Foreign_born
    ,Average_Household_Size
    From staging.us_demographics;
    """)

    insert_accident_table = ("""
        Select ID AS Accident_ID
        ,Description
        ,Severity
        ,Start_Time
        ,End_Time
        ,Timezone
        ,Start_Lat
        ,Start_Lng
        ,Source
        ,md5(City || State)::varchar(32) City_ID
        ,md5(Coalesce(Number,'')::varchar(256)
        || Coalesce(Street,'')::varchar(256)
        || Coalesce(Side,'')::varchar(256)
        || Coalesce(City,'')::varchar(256)
        || Coalesce(County,'')::varchar(256)
        || Coalesce(State,'')::varchar(256)
        || Coalesce(Zipcode,'')::varchar(256)
        || Coalesce(Country,'')::varchar(256))::varchar(32) As ACCIDENT_ADDRESS_ID
        ,md5(Start_Lat::varchar(256) || Start_Lng::varchar(256))::varchar(32) As ACCIDENT_LOCATION_DETAILS_ID
        ,extract('epoch' from Weather_Timestamp)::varchar || Airport_Code As Weather_Condition_ID
        ,extract('epoch' from Start_Time::Date) As Date_ID
        From staging.us_accidents;
        """)

    insert_covid_table = ("""
    Select Row_Number() Over(order by true) As Covid_ID
	,B.State_Code AS State
    ,A.Admin2 As County
    ,A.Date
    ,Sum(A.Confirmed) As Confirmed
    ,Sum(A.Deaths) As Deaths
    ,extract('epoch' from A.Date) As Date_ID
    From staging.us_covid_19 As A
    Left Join (Select Distinct State, State_Code From staging.us_demographics) As B On A.Province_State = B.State
    Group By B.State_Code
            ,A.Admin2
            ,A.Date
            ,extract('epoch' from A.Date)
    """)