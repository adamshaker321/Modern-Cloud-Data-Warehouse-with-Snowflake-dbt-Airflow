WITH date_range AS (

    SELECT
        MIN(ORDER_DATE) AS START_DATE,
        GREATEST(
            MAX(ORDER_DATE),
            MAX(REQUIRED_DATE),
            MAX(SHIPPED_DATE)
        ) AS END_DATE

    FROM {{ ref('orders') }}

),

dates AS (

    SELECT
        DATEADD(
            DAY,
            SEQ4(),
            START_DATE
        ) AS FULL_DATE,
        END_DATE

    FROM date_range,
         TABLE(GENERATOR(ROWCOUNT => 5000))

)

SELECT

    TO_NUMBER(TO_CHAR(FULL_DATE, 'YYYYMMDD')) AS DATE_KEY,

    FULL_DATE,

    YEAR(FULL_DATE) AS YEAR,

    QUARTER(FULL_DATE) AS QUARTER,

    MONTH(FULL_DATE) AS MONTH,

    TO_CHAR(FULL_DATE, 'MMMM') AS MONTH_NAME,
    
    DAY(FULL_DATE) AS DAY,

    DAYNAME(FULL_DATE) AS DAY_NAME,

    WEEK(FULL_DATE) AS WEEK

FROM dates

WHERE FULL_DATE <= END_DATE
ORDER BY FULL_DATE