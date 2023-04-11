import time

import sqlparse
from sqlparse.sql import Identifier
from sqlparse.tokens import DML, Name, Keyword, DDL
import sql_metadata


sql_code = """
        -- 从 dw.dwd_spgl_ccp_proj_construction_permit 补充项目代码和工程代码
        with temp as (
        select *
        from dw.dwd_spgl_ccp_proj_construction_permit
        where gcdm in (select gcdm
                       from dw.dwd_spgl_ccp_proj_construction_permit
                       group by gcdm
                       having count(1) = 1))
        update dw.dwm_sgxkz_ggpp sg
        set (prj_code_auto,sub_prj_code_auto) = (select substr(gcdm,1,24) as xmdm , gcdm
                                                 from temp t
                                                 where t.sgxkzbh = sg.builders_license
                                                 and gcdm not like '%FS%')
        where exists(select 1 from temp t where t.sgxkzbh = sg.builders_license and gcdm not like '%FS%');

        with temp as (
        select *
        from dw.dwd_spgl_ccp_proj_construction_permit
        where gcdm in (select gcdm
                       from dw.dwd_spgl_ccp_proj_construction_permit
                       group by gcdm
                       having count(1) = 1))
        update dw.dwm_sgxkz_ggpp sg
        set (prj_code_auto,sub_prj_code_auto) = (select substr(gcdm,1,30) as xmdm , gcdm
                                                 from temp t
                                                 where t.sgxkzbh = sg.builders_license
                                                 and gcdm like '%FS%')
        where exists(select 1 from temp t where t.sgxkzbh = sg.builders_license and gcdm like '%FS%');

        -- 从 ods.spgl_ccp_proj_construction_permit2 补充项目代码和工程代码
        update dw.dwm_sgxkz_ggpp sg
        set (prj_code_auto,sub_prj_code_auto) = (select substr(gcdm,1,24) as xmdm , gcdm
                                                 from ods.spgl_ccp_proj_construction_permit2 t
                                                 where t.sgxkzbh = sg.builders_license
                                                 and gcdm not like '%FS%')
        where sub_prj_code_auto isnull
          and exists(select 1 from ods.spgl_ccp_proj_construction_permit2 t where t.sgxkzbh = sg.builders_license and gcdm not like '%FS%');

        update dw.dwm_sgxkz_ggpp sg
        set (prj_code_auto,sub_prj_code_auto) = (select substr(gcdm,1,30) as xmdm , gcdm
                                                 from ods.spgl_ccp_proj_construction_permit2 t
                                                 where t.sgxkzbh = sg.builders_license
                                                 and gcdm like '%FS%')
        where sub_prj_code_auto isnull
          and exists(select 1 from ods.spgl_ccp_proj_construction_permit2 t where t.sgxkzbh = sg.builders_license and gcdm like '%FS%');

        -- 从 dw.dwd_sgxkz_sgzhwfzrzt 补充项目代码和工程代码
        update dw.dwm_sgxkz_ggpp s
        set (prj_code_auto, sub_prj_code_auto) = (select prj_code, sub_prj_code
                                                  from dw.dwd_sgxkz_sgzhwfzrzt t
                                                  where t.builders_license = s.builders_license)
        where sub_prj_code_auto isnull
          and exists(select 1
                     from dw.dwd_sgxkz_sgzhwfzrzt t
                     where t.builders_license = s.builders_license);

        -- 从 dw.dwd_sgxkz_2020_2022 补充项目代码和工程代码
        update dw.dwm_sgxkz_ggpp s
        set (prj_code_auto, sub_prj_code_auto) = (select prj_code, sub_prj_code
                                                  from dw.dwd_sgxkz_2020_2022 t
                                                  where t.builders_license = s.builders_license)
        where sub_prj_code_auto isnull
          and exists(select 1
                     from dw.dwd_sgxkz_2020_2022 t
                     where t.builders_license = s.builders_license);

        -- 从附件中补充项目代码和工程代码
        update dw.dwm_sgxkz_ggpp a
        set (prj_code_auto , sub_prj_code_auto ) = (select 工改项目代码 , 工改工程代码
                                                    from dw.dwd_sgxkz_fj b
                                                    where a.builders_license = b.施工许可证
                                                      and b.施工许可证 is not null)
        where sub_prj_code_auto isnull
          and exists(select 1 from dw.dwd_sgxkz_fj b where a.builders_license = b.施工许可证) ;

       -- 补充2019年之后无项目代码的施工许可证的项目代码和部分工程代码
       update dw.dwm_sgxkz_ggpp a
       set (prj_code_auto,sub_prj_code_auto) = (select b.项目代码 , b.工程代码
                                                from dw.temp_sgxkz_2019xmdm b
                                                where a.builders_license = b.施工许可证
                                                  and b.项目代码 is not null)
       where sub_prj_code_auto isnull
         and exists(select 1 from dw.temp_sgxkz_2019xmdm b where a.builders_license = b.施工许可证 and b.项目代码 is not null) ;

        -- 更新2022年的施工许可证的人工匹配数据
        update dw.dwm_sgxkz_ggpp a
        set (prj_code_auto,sub_prj_code_auto) = (select prj_code,sub_prj_code
                                                 from dw.temp_sgxkz_sd_2022 b
                                                 where b.sub_prj_code is not null
                                                   and a.builders_license = b.build_licence_num)
        where exists(select 1
                     from dw.temp_sgxkz_sd_2022 b
                     where b.sub_prj_code is not null
                       and a.builders_license = b.build_licence_num);

        -- 人工匹配的
        -- 2022年的
        insert into dw.dwm_sgxkz_ggpp (prj_code_auto,
                                       sub_prj_code_auto,
                                       sub_prj_name,
                                       builders_license,
                                       construction_address,
                                       date_of_issue,
                                       contract_price)
        select substr("工改工程编码", 1, 24) as prj_code_auto
             , "工改工程编码"                as sub_prj_code_auto
             , "工程名称"                  as sub_prj_name
             , "手动更新的施许可证"             as builders_license
             , "工程地址"                  as construction_address
             , b.date_of_issue
             , b.contract_price
        from dw.temp_dm_smz_engineering_info_zh_2022_xz a
        left join dw.dwm_sgxkz_jbxx b
          on a.手动更新的施许可证 = b.builders_license ;

        -- 2021年
        insert into dw.dwm_sgxkz_ggpp (prj_code_auto,
                                       sub_prj_code_auto,
                                       sub_prj_name,
                                       builders_license,
                                       construction_address,
                                       date_of_issue,
                                       contract_price)
        select substr("工改工程编码", 1, 24) as prj_code_auto
             , "工改工程编码"                as sub_prj_code_auto
             , "工程名称"                  as sub_prj_name
             , "手动更新的施许可证"             as builders_license
             , "工程地址"                  as construction_address
             , b.date_of_issue
             , b.contract_price
        from dw.temp_smz_engineering_info_zh_2021_xz a
        left join dw.dwm_sgxkz_jbxx b
          on a.手动更新的施许可证 = b.builders_license ;

        -- 补充项目代码
        -- 通过项目名称补充项目代码
        with temp as (select xmdm , xmmc
                      from dw.dwd_spgl_xmjbxx
                      group by xmdm , xmmc)
        update dw.dwm_sgxkz_ggpp sg
        set prj_code_force = (select jc.xmdm
                              from temp jc
                              where sg.prj_name = jc.xmmc
                              order by jc.xmdm
                              limit 1)
        where prj_code_force isnull
          and length(prj_name) > 3
          and exists(select 1
                     from temp jc
                     where sg.prj_name = jc.xmmc);
        -- 项目名称长度太短的,再加上建设地址来匹配
        update dw.dwm_sgxkz_ggpp sg
        set prj_code_force = (select jc.xmdm
                              from dw.dwd_spgl_xmjbxx jc
                              where sg.prj_name = jc.xmmc
                                and sg.construction_address = jc.jsdd
                              order by jc.xmdm
                              limit 1)
        where prj_code_force isnull
          and length(prj_name) <= 3
          and exists(select 1
                     from dw.dwd_spgl_xmjbxx jc
                     where sg.prj_name = jc.xmmc
                       and sg.construction_address = jc.jsdd);

        -- 使用 建设工程规划许可证编号 匹配项目代码
        update dw.dwm_sgxkz_ggpp dsg
        set prj_code_force = (select xmdm
                              from dw.dwd_spgl_xmjbxx jpe
                              where jpe.project_pemit_id = dsg.prj_land_planning_permit
                              order by xmdm
                              limit 1)
        where prj_code_force isnull
          and exists(select 1
                     from dw.dwd_spgl_xmjbxx jpe
                     where jpe.project_pemit_id = dsg.prj_land_planning_permit);



        -- 补充工程代码
        -- 通过工程名称和项目代码补充工程代码
        update dw.dwm_sgxkz_ggpp sg
        set sub_prj_code_force = (select jc.gcdm
                                  from dw.dwd_spgl_xmjbxx jc
                                  where sg.sub_prj_name = jc.gcfw
                                    and jc.xmdm = coalesce(sg.prj_code_auto, sg.sub_prj_code_force)
                                  order by jc.gcdm
                                  limit 1)
        where sub_prj_code_force isnull
          and exists(select 1
                     from dw.dwd_spgl_xmjbxx jc
                     where sg.sub_prj_name = jc.gcfw
                       and jc.xmdm = coalesce(sg.prj_code_auto, sg.sub_prj_code_force));
        -- 通过处理后的工程名称和项目代码补充工程代码
        update dw.dwm_sgxkz_ggpp sg
        set sub_prj_code_force = (select jc.gcdm
                                  from dw.dwd_spgl_xmjbxx jc
                                  where regexp_replace(sg.sub_prj_name, '\W', '', 'g') = regexp_replace(jc.gcfw, '\W', '', 'g')
                                    and jc.xmdm = coalesce(sg.prj_code_auto, sg.sub_prj_code_force)
                                  order by jc.gcdm
                                  limit 1)
        where sub_prj_code_force isnull
          and exists(select 1
                     from dw.dwd_spgl_xmjbxx jc
                     where regexp_replace(sg.sub_prj_name, '\W', '', 'g') = regexp_replace(jc.gcfw, '\W', '', 'g')
                       and jc.xmdm = coalesce(sg.prj_code_auto, sg.sub_prj_code_force));

        /*-- 使用 项目代码和建设工程规划许可证编号 匹配工程代码  匹配的数据存在问题
        update dw.dwm_sgxkz_ggpp dsg
        set sub_prj_code_force = (select gcdm
                                  from dw.dwd_spgl_xmjbxx jpe
                                  where jpe.xmdm = coalesce(dsg.prj_code_auto, dsg.prj_code_force)
                                    and jpe.project_pemit_id = dsg.prj_land_planning_permit
                                  order by gcdm
                                  limit 1)
        where sub_prj_code_force isnull
          and exists(select 1
                     from dw.dwd_spgl_xmjbxx jpe
                     where jpe.project_pemit_id = dsg.prj_land_planning_permit);
        update dw.dwm_sgxkz_ggpp dsg
        set sub_prj_code_force = (select gcdm
                                  from dw.dwd_spgl_xmjbxx jpe
                                  where jpe.xmdm = coalesce(dsg.prj_code_auto, dsg.prj_code_force)
                                    and regexp_replace(jpe.project_pemit_id, '[^0-9]{1}', '', 'g')
                                      = regexp_replace(dsg.prj_land_planning_permit, '[^0-9]{1}', '', 'g')
                                  order by gcdm
                                  limit 1)
        where sub_prj_code_force isnull
          and exists(select 1
                     from dw.dwd_spgl_xmjbxx jpe
                     where regexp_replace(jpe.project_pemit_id, '[^0-9]{1}', '', 'g')
                               = regexp_replace(dsg.prj_land_planning_permit, '[^0-9]{1}', '', 'g'));*/

        -- 通过工程名称补充工程代码
        update dw.dwm_sgxkz_ggpp sg
        set sub_prj_code_force = (select jc.gcdm
                                  from dw.dwd_spgl_xmjbxx jc
                                  where regexp_replace(sg.sub_prj_name, '\W', '', 'g') = regexp_replace(jc.gcfw, '\W', '', 'g')
                                  order by jc.gcdm
                                  limit 1)
        where sub_prj_code_force isnull
          and length(sub_prj_name) > 10
          and exists(select 1
                     from dw.dwd_spgl_xmjbxx jc
                     where regexp_replace(sg.sub_prj_name, '\W', '', 'g') = regexp_replace(jc.gcfw, '\W', '', 'g'));
        -- 工程名称长度太短的,再加上建设地址来匹配
        update dw.dwm_sgxkz_ggpp sg
        set sub_prj_code_force = (select jc.gcdm
                                  from dw.dwd_spgl_xmjbxx jc
                                  where regexp_replace(sg.sub_prj_name, '\W', '', 'g') = regexp_replace(jc.gcfw, '\W', '', 'g')
                                    and sg.construction_address = jc.jsdd
                                  order by jc.gcdm
                                  limit 1)
        where sub_prj_code_force isnull
          and length(sub_prj_name) <= 10
          and exists(select 1
                     from dw.dwd_spgl_xmjbxx jc
                     where regexp_replace(sg.sub_prj_name, '\W', '', 'g') = regexp_replace(jc.gcfw, '\W', '', 'g')
                       and sg.construction_address = jc.jsdd);



"""

print(sqlparse.split(sql_code))
for index, sql in enumerate(sqlparse.split(sql_code)):
    print(index)
    sql = sqlparse.format(sql, strip_comments=True)
    stmt = sqlparse.parse(sql)[0].tokens
    for token in stmt:
        print(type(token), token.ttype, token.value)
        if token.ttype is DDL or token.ttype is DML:
            print(token.value)
        if type(token).__name__ == 'Identifier':

            print(token.value.split("(")[0])

            # print(token.value)
            # print(token.value)
    # stmt = sqlparse.parse(sql)[0].tokens
    # for token in stmt:
    #     print(type(token), token.ttype, token.value)

        # if token.ttype is Keyword:
        #     print(token.value)

