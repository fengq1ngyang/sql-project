import re
import threading
import time
from sqllineage.runner import *
import sqlparse
import DBUtil
import logging
from multiprocessing import Queue
from  multiprocessing import Process
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import atexit


# print(nums.pop())
item = """
INSERT INTO dw.dwd_spgl_spzt WITH temp1 AS
  (SELECT d.lcbsxlx,
          d.dybzspjdxh,
          d.spjdmc,
          c.sflcbsx,
          c.dybzspsxbm,
          c.spsxmc,
          a.lsh AS spsx_blxx_lsh,
          a.blzt,
          a.blztmc,
          a.blcs,
          a.blr,
          a.blsj,
          a.blyj,
          b.spbmmc,
          b.spsxslbm,
          b.gcdm,
          b.splcbm,
          b.splcbbh,
          b.spsxbm,
          b.spsxbbh,
          row_number() over(PARTITION BY b.gcdm, b.spsxslbm
                            ORDER BY a.blsj DESC) AS n
   FROM dw.dwd_spgl_blxx b
   LEFT JOIN dw.dwd_spgl_blxxxx a ON a.spsxslbm = b.spsxslbm
   JOIN dw.dwd_spgl_jdsxxx c ON b.splcbm=c.splcbm
   AND b.splcbbh=c.splcbbh
   AND b.spsxbbh=c.spsxbbh
   AND b.spsxbm=c.spsxbm
   JOIN dw.dwd_spgl_jdxx d ON d.splcbm=c.splcbm
   AND d.splcbbh=c.splcbbh
   AND d.spjdxh = c.spjdxh)
SELECT lcbsxlx,
       dybzspjdxh,
       spjdmc,
       sflcbsx,
       dybzspsxbm,
       spsxmc,
       spsx_blxx_lsh,
       blzt,
       blztmc,
       blcs,
       blr,
       blsj,
       blyj,
       spbmmc,
       spsxslbm,
       gcdm,
       splcbm,
       splcbbh,
       spsxbm,
       spsxbbh
FROM temp1
WHERE n = 1;
"""

try:
    item = sqlparse.format(item, reindent=True, keyword_case='upper', strip_comments=True)
    result = LineageRunner(item, verbose=True)
    if len(result.source_tables) > 0 and len(result.target_tables) > 0:
        for source in result.source_tables:
            match_obj = re.search(r'<default>', f"{source.schema}.{source.raw_name}")
            if match_obj:
                pass
            else:
                for target in result.target_tables:
                    source_table = f"{source.schema}.{source.raw_name}"
                    source_model = f"{source.schema}"
                    target_table = f"{target.schema}.{target.raw_name}"
                    target_model = f"{target.schema}"
                    function_name =1
                    work_step =1
                    work_id = 1
                    execution_time = 1
                    sql_row_number = 1
                    sql_code = str(f"{item}").strip().replace("'", "''")
                    data_obj = [source_table, source_model, target_table, target_model, function_name,
                                work_step, sql_code, sql_row_number, work_id, None, execution_time]
                    print(data_obj)
except Exception as e:
    error = [None, None, None, None, 1, 1, str(f"{item}").strip().replace("'", "''"), 1, 1,
             str(e), 1]
