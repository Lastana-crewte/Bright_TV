WITH Viewership_Clean AS (
    SELECT 
        CAST(_c0 AS STRING)       AS UserID,
        TRIM(CAST(_c1 AS STRING)) AS Channel,
        CAST(TO_TIMESTAMP(_c2, 'M/d/yy H:mm') AS DATE) AS RecordDate,
        COALESCE((CAST(split(_c3, ':')[0] AS INT) * 60 + CAST(split(_c3, ':')[1] AS INT)), 0) AS Duration,
        from_utc_timestamp(TO_TIMESTAMP(_c2, 'M/d/yy H:mm'), 'Africa/Johannesburg') AS converted_recording_time,
        CAST(from_utc_timestamp(TO_TIMESTAMP(_c2, 'M/d/yy H:mm'), 'Africa/Johannesburg') AS DATE) AS duration_date,
        date_format(from_utc_timestamp(TO_TIMESTAMP(_c2, 'M/d/yy H:mm'), 'Africa/Johannesburg'), 'HH:mm:ss') AS duration_SA_time
    FROM read_files('/Volumes/bright_tv/default/viewership/Viewership.xlsx')
    WHERE _c0 != 'UserID'
),

UserProfile_Clean AS (
    SELECT 
        CAST(_c0 AS STRING) AS UserID,
        COALESCE(TRY_CAST(NULLIF(_c6, 'None') AS INT), TRY_CAST(NULLIF(_c6, '0') AS INT), 0) AS Age,
        CASE 
            WHEN TRIM(_c4) IN ('None', '', 'NULL') OR TRIM(_c4) IS NULL THEN 'Others'
            ELSE TRIM(_c4)
        END AS Gender,
        CASE 
            WHEN TRIM(_c7) IN ('None', '', 'NULL') OR TRIM(_c7) IS NULL THEN 'Not specified'
            ELSE TRIM(_c7)
        END AS Location
    FROM read_files('/Volumes/bright_tv/default/viewership/User_Profile.xlsx')
    WHERE _c0 != 'UserID'
),

Joined_Data AS (
    SELECT 
        v.UserID,
        v.Channel,
        v.RecordDate,
        v.Duration,
        v.converted_recording_time,
        v.duration_date,
        v.duration_SA_time,
        u.Age,
        u.Gender,
        u.Location
    FROM Viewership_Clean v
    LEFT JOIN UserProfile_Clean u
        ON v.UserID = u.UserID
),

Enriched_Data AS (
    SELECT *,

    -- Age Group
    CASE 
        WHEN Age < 18 THEN 'Under 18'
        WHEN Age BETWEEN 18 AND 24 THEN '18-24'
        WHEN Age BETWEEN 25 AND 34 THEN '25-34'
        WHEN Age BETWEEN 35 AND 44 THEN '35-44'
        WHEN Age BETWEEN 45 AND 54 THEN '45-54'
        WHEN Age > 0 THEN '55+'
        ELSE 'Not specified'
    END AS Age_Group,

    -- Engagement
    CASE 
        WHEN Duration < 30 THEN 'Low'
        WHEN Duration BETWEEN 30 AND 120 THEN 'Medium'
        ELSE 'High'
    END AS Engagement_Level,

    -- Day Type
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM RecordDate) IN (1,7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS Day_Type,

    date_format(RecordDate, 'MMM') AS Month,

    -- Power Users
    CASE 
        WHEN Duration > 180 THEN 'Power User'
        ELSE 'Normal User'
    END AS User_Type

    FROM Joined_Data
),

User_Value AS (
    SELECT 
        UserID,
        SUM(Duration) AS Total_User_Time,
        COUNT(*)      AS Total_Views
    FROM Enriched_Data
    GROUP BY UserID
)

-- ===============================
-- FINAL SINGLE DATASET
-- ===============================
SELECT 
    e.UserID,
    e.Channel,
    e.RecordDate,
    e.converted_recording_time,
    e.duration_date,
    e.duration_SA_time,
    e.Month,
    e.Day_Type,
    e.Duration,

    e.Age,
    e.Gender,
    e.Location,
    e.Age_Group,

    e.Engagement_Level,
    e.User_Type,

    uv.Total_User_Time,
    uv.Total_Views,

    -- Customer Value Segment (CEO metric)
    CASE 
        WHEN uv.Total_User_Time > 500 THEN 'VIP'
        WHEN uv.Total_User_Time BETWEEN 200 AND 500 THEN 'Loyal'
        ELSE 'Casual'
    END AS Customer_Segment,

    -- Channel Market Share
    SUM(e.Duration) OVER (PARTITION BY e.Channel) AS Channel_Total_Time,

    -- Overall Total Time
    SUM(e.Duration) OVER () AS Platform_Total_Time,

    -- Channel Contribution %
    ROUND(
        100 * SUM(e.Duration) OVER (PARTITION BY e.Channel) 
        / SUM(e.Duration) OVER (), 2
    ) AS Channel_Market_Share_Percent,

    -- User Contribution %
    ROUND(
        100 * uv.Total_User_Time 
        / SUM(e.Duration) OVER (), 2
    ) AS User_Contribution_Percent,

    -- Rank Channels by Performance
    RANK() OVER (ORDER BY SUM(e.Duration) OVER (PARTITION BY e.Channel) DESC) 
        AS Channel_Rank,

    -- Rank Users by Value
    RANK() OVER (ORDER BY uv.Total_User_Time DESC) 
        AS User_Rank

FROM Enriched_Data e
LEFT JOIN User_Value uv
    ON e.UserID = uv.UserID;
