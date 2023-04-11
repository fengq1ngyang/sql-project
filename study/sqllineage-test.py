from sqllineage.runner import LineageRunner

sql_code = """
INSERT INTO
  dm_business_sys.t_education_user_activation(
    temporal_classification,
    corp_code,
    prj_code,
    input_users,
    user_activation,
    TIME,
    id,
    creator_id,
    create_date,
    modifier_id,
    modify_time,
    is_deleted
  )
SELECT
  ' seven_days ' :: VARCHAR AS temporal_classification,
  jc.corp_code,
  jc.prj_code,
  jc.导入用户数,
  jc.每天参考人数 + jc.每天参训人数 AS 活跃度,
  exam.exam_date,
  uuid_generate_v4() AS id,
  ' sjzt ' AS creator_id,
  now() AS create_date,
  ' sjzt ' AS modifier_id,
  now() AS modify_time,
  0 AS is_deleted
FROM
  (
    SELECT
      e.prj_code,
      c.corp_code,
      count(DISTINCT b.certificate_code) AS 导入用户数,
      count(DISTINCT f.staff_id) AS 每天参考人数,
      count(DISTINCT g.staff_id) AS 每天参训人数
    FROM
      ods.basic_jc_person_work_info a
      JOIN ods.basic_t_person b ON a.idcard_num = b.certificate_code
      JOIN ods.basic_jc_corp_charger c ON c.person_idcard = b.certificate_code
      JOIN ods.basic_jc_project_person_info d ON d.idcard_num = c.person_idcard
      JOIN ods.basic_jc_project_info e ON e.prj_code = d.prj_code
      LEFT JOIN ods.education_t_exam_record f ON f.staff_id = a.idcard_num
      LEFT JOIN ods.education_t_course_train_record g ON g.staff_id = a.idcard_num
      AND a.is_deleted = 0
      AND b.is_deleted = 0
      AND c.is_deleted = 0
      AND d.is_deleted = 0
      AND e.is_deleted = 0
      AND f.is_deleted = 0
    GROUP BY
      e.prj_code,
      c.corp_code
  ) jc,
  (
    SELECT
      generate_series(
        date_trunc(' day' , now()) + ' -6 day ',
        now(),
        '1 day '    
      ) AS exam_date
  ) exam
UNION
SELECT
  ' seven_days ' :: VARCHAR AS temporal_classification,
  jc.corp_code,
  jc.prj_code,
  jc.导入用户数,
  jc.每天参考人数 + jc.每天参训人数 AS 活跃度,
  exam.exam_date,
  uuid_generate_v4() AS id,
  'sjzt ' AS creator_id,
  now() AS create_date,
  ' sjzt ' AS modifier_id,
  now() AS modify_time,
  0 AS is_deleted
FROM
  (
    SELECT
      NULL AS prj_code,
      NULL AS corp_code,
      (
        SELECT
          count(DISTINCT certificate_code)
        FROM
          ods.basic_t_person a
          JOIN ods.basic_jc_person_work_info b ON b.idcard_num = a.certificate_code
        WHERE
          a.is_deleted = 0
          AND b.is_deleted = 0
      ) AS 导入用户数,
      count(DISTINCT f.staff_id) AS 每天参考人数,
      count(DISTINCT g.staff_id) AS 每天参训人数
    FROM
      ods.basic_jc_person_work_info a
      JOIN ods.basic_t_person b ON a.idcard_num = b.certificate_code
      JOIN ods.basic_jc_corp_charger c ON c.person_idcard = b.certificate_code
      JOIN ods.basic_jc_project_person_info d ON d.idcard_num = c.person_idcard
      JOIN ods.basic_jc_project_info e ON e.prj_code = d.prj_code
      LEFT JOIN ods.education_t_exam_record f ON f.staff_id = a.idcard_num
      LEFT JOIN ods.education_t_course_train_record g ON g.staff_id = a.idcard_num
      AND a.is_deleted = 0
      AND b.is_deleted = 0
      AND c.is_deleted = 0
      AND d.is_deleted = 0
      AND e.is_deleted = 0
      AND f.is_deleted = 0
  ) jc,
  (
    SELECT
      generate_series(
        date_trunc(' day ', now()) + ' -6 day ',
        now(),
        ' 1 day '
      ) AS exam_date
  ) exam
UNION
SELECT
  ' seven_days ' :: VARCHAR AS temporal_classification,
  jc.corp_code,
  jc.prj_code,
  jc.导入用户数,
  jc.每天参考人数 + jc.每天参训人数 AS 活跃度,
  exam.exam_date,
  uuid_generate_v4() AS id,
  ' sjzt ' AS creator_id,
  now() AS create_date,
  ' sjzt ' AS modifier_id,
  now() AS modify_time,
  0 AS is_deleted
FROM
  (
    SELECT
      NULL AS prj_code,
      c.corp_code,
      count(DISTINCT b.certificate_code) AS 导入用户数,
      count(DISTINCT f.staff_id) AS 每天参考人数,
      count(DISTINCT g.staff_id) AS 每天参训人数
    FROM
      ods.basic_jc_person_work_info a
      JOIN ods.basic_t_person b ON a.idcard_num = b.certificate_code
      JOIN ods.basic_jc_corp_charger c ON c.person_idcard = b.certificate_code
      JOIN ods.basic_jc_project_person_info d ON d.idcard_num = c.person_idcard
      JOIN ods.basic_jc_project_info e ON e.prj_code = d.prj_code
      LEFT JOIN ods.education_t_exam_record f ON f.staff_id = a.idcard_num
      LEFT JOIN ods.education_t_course_train_record g ON g.staff_id = a.idcard_num
      AND a.is_deleted = 0
      AND b.is_deleted = 0
      AND c.is_deleted = 0
      AND d.is_deleted = 0
      AND e.is_deleted = 0
      AND f.is_deleted = 0
    GROUP BY
      c.corp_code
  ) jc,
  (
    SELECT
      generate_series(
        date_trunc(' day ', now()) + ' -6 day ',
        now(),
        ' 1 day '
      ) AS exam_date
  ) exam;
"""
import sqlparse
sql_code = sqlparse.format(sql_code, reindent=True, keyword_case='upper', strip_comments=True)
result = LineageRunner(sql_code,verbose=True)
print(result)

