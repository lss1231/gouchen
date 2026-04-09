-- 日期维度表数据
-- 生成 2020-01-01 到 2025-12-31 的日期数据
-- 字段：date_key, date, year, month, day, quarter, week_of_year, day_of_week, is_weekend, is_holiday

-- 使用 MySQL 存储过程生成日期数据
DELIMITER $$

DROP PROCEDURE IF EXISTS GenerateDimDate$$

CREATE PROCEDURE GenerateDimDate()
BEGIN
    DECLARE start_date DATE DEFAULT '2020-01-01';
    DECLARE end_date DATE DEFAULT '2025-12-31';
    DECLARE current_date DATE DEFAULT start_date;
    DECLARE v_date_key INT;
    DECLARE v_year SMALLINT;
    DECLARE v_month TINYINT;
    DECLARE v_day TINYINT;
    DECLARE v_quarter TINYINT;
    DECLARE v_week_of_year TINYINT;
    DECLARE v_day_of_week TINYINT;
    DECLARE v_is_weekend BOOLEAN;
    DECLARE v_is_holiday BOOLEAN;
    DECLARE v_holiday_name VARCHAR(50);

    WHILE current_date <= end_date DO
        SET v_date_key = DATE_FORMAT(current_date, '%Y%m%d');
        SET v_year = YEAR(current_date);
        SET v_month = MONTH(current_date);
        SET v_day = DAY(current_date);
        SET v_quarter = QUARTER(current_date);
        SET v_week_of_year = WEEK(current_date, 1);
        SET v_day_of_week = DAYOFWEEK(current_date);
        SET v_is_weekend = IF(v_day_of_week IN (1, 7), 1, 0);

        -- 判断节假日（简化版，主要节假日）
        SET v_is_holiday = 0;
        SET v_holiday_name = NULL;

        -- 元旦
        IF MONTH(current_date) = 1 AND DAY(current_date) = 1 THEN
            SET v_is_holiday = 1;
            SET v_holiday_name = '元旦';
        -- 春节（简化处理，实际日期每年不同）
        ELSEIF (MONTH(current_date) = 1 AND DAY(current_date) >= 21) OR
               (MONTH(current_date) = 2 AND DAY(current_date) <= 20) THEN
            SET v_is_holiday = 1;
            SET v_holiday_name = '春节';
        -- 清明节（4月4日-6日）
        ELSEIF MONTH(current_date) = 4 AND DAY(current_date) IN (4, 5, 6) THEN
            SET v_is_holiday = 1;
            SET v_holiday_name = '清明节';
        -- 劳动节
        ELSEIF MONTH(current_date) = 5 AND DAY(current_date) = 1 THEN
            SET v_is_holiday = 1;
            SET v_holiday_name = '劳动节';
        -- 端午节（简化处理）
        ELSEIF MONTH(current_date) = 6 AND DAY(current_date) IN (10, 11, 12) THEN
            SET v_is_holiday = 1;
            SET v_holiday_name = '端午节';
        -- 中秋节（简化处理）
        ELSEIF MONTH(current_date) = 9 AND DAY(current_date) IN (15, 16, 17) THEN
            SET v_is_holiday = 1;
            SET v_holiday_name = '中秋节';
        -- 国庆节
        ELSEIF MONTH(current_date) = 10 AND DAY(current_date) IN (1, 2, 3, 4, 5, 6, 7) THEN
            SET v_is_holiday = 1;
            SET v_holiday_name = '国庆节';
        END IF;

        INSERT INTO dim_date (date_key, date, year, month, day, quarter, week_of_year, day_of_week, is_weekend, is_holiday, holiday_name)
        VALUES (v_date_key, current_date, v_year, v_month, v_day, v_quarter, v_week_of_year, v_day_of_week, v_is_weekend, v_is_holiday, v_holiday_name);

        SET current_date = DATE_ADD(current_date, INTERVAL 1 DAY);
    END WHILE;
END$$

DELIMITER ;

-- 执行存储过程生成数据
CALL GenerateDimDate();

-- 删除存储过程
DROP PROCEDURE IF EXISTS GenerateDimDate;
