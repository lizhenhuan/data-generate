package com.pingcap.data.generate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class TestPreparedOnce1 {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerate.class);
    private static int THREAD_NUM;
    private static AtomicInteger TOTAL_SIZE = new AtomicInteger();
    private static String IP_PORT;
    private static String USER;
    private static String PASSWORD;
    private static String DB_NAME;
    private static final String SQL = "WITH \n" +
            "mzjcsl\n" +
            "as (\n" +
            "SELECT jcsl.yljgdm,jcsl.jzxh,jcsl.jzlb\n" +
            "FROM  jc_bgzb jcsl  \n" +
            "WHERE \n" +
            "jcsl.hzid in ('70360152') \n" +
            "AND jcsl.bgzt IN ( '06', '07' ) \n" +
            "AND jcsl.isdelete = '0' \n" +
            "),\n" +
            "zyjcsl\n" +
            "as (\n" +
            "SELECT jcsl.yljgdm,jcsl.jzxh\n" +
            "FROM  jc_bgzb jcsl  \n" +
            "WHERE \n" +
            "jcsl.hzid in ('70360152') \n" +
            "AND jcsl.bgzt IN ( '06', '07' ) \n" +
            "and jcsl.jzlb='03'\n" +
            "AND jcsl.isdelete = '0' \n" +
            "),\n" +
            "mzgmsl\n" +
            "as (\n" +
            "SELECT gmsl.yljgdm,gmsl.jzxh,gmsl.jzlb\n" +
            "FROM  hz_gm_gmxx gmsl \n" +
            "WHERE \n" +
            "gmsl.hzid in ('70360152') \n" +
            "and gmsl.isdelete = '0' \n" +
            "),\n" +
            "zygmsl\n" +
            "as (\n" +
            "SELECT gmsl.yljgdm,gmsl.jzxh,gmsl.jzlb\n" +
            "FROM  hz_gm_gmxx gmsl\n" +
            "WHERE \n" +
            "gmsl.hzid in ('70360152') \n" +
            "and gmsl.isdelete = '0' \n" +
            "),\n" +
            "cte_all AS (\n" +
            "SELECT\n" +
            "CAST( ' ' AS CHAR ) opentype,\n" +
            "--  -- 打开方式 新窗口:_blank 内嵌:iframe_page\n" +
            "CAST( ' ' AS CHAR ) url,\n" +
            "--  -- 链接\n" +
            "a.jzxh,\n" +
            "--  -- 就诊序号\n" +
            "a.hzid,\n" +
            "--  -- 患者id\n" +
            "a.yljgdm,\n" +
            "--  -- 医疗机构代码\n" +
            "a.yljgmc,\n" +
            "--  -- 医疗机构名称\n" +
            "a.ghysdm ysdm,\n" +
            "--  -- 医生代码\n" +
            "a.ghysmc ysmc,\n" +
            "--  -- 医生名称\n" +
            "a.jzlb,\n" +
            "--  -- 就诊类别\n" +
            "CAST( CASE WHEN a.jzlb = '1' THEN '门诊' ELSE '急诊' END AS CHAR ) jzlbzw,\n" +
            "--  -- 就诊类别名称\n" +
            "hz.hzsfzh hzsfzh,\n" +
            "--  -- 患者身份证\n" +
            "a.ghksdm ksdm,\n" +
            "--  -- 挂号科室名称\n" +
            "a.ghksmc ksmc,\n" +
            "--  -- 挂号科室名称\n" +
            "CAST( ' ' AS CHAR ) bqdm,\n" +
            "--  -- 病区\n" +
            "CAST( ' ' AS CHAR ) bqmc,\n" +
            "--  -- 病区\n" +
            "CAST( ' ' AS CHAR ) cwh,\n" +
            "--  -- 床位\n" +
            "a.ghsj rysj,\n" +
            "--  -- 挂号时间\n" +
            "a.ghsj cysj,\n" +
            "--  -- 挂号时间\n" +
            "CAST( '0' AS CHAR ) zyts,\n" +
            "-- -- 住院天数\n" +
            "(\n" +
            "SELECT\n" +
            "b.ynzddm \n" +
            "FROM\n" +
            "JZ_MJZ_ZDJL b \n" +
            "WHERE\n" +
            "b.jzxh = a.jzxh \n" +
            "AND b.jzlb = a.jzlb \n" +
            "AND b.yljgdm = a.yljgdm \n" +
            "LIMIT 1 \n" +
            ") AS zddm,\n" +
            "(\n" +
            "SELECT\n" +
            "b.ynzdmc \n" +
            "FROM\n" +
            "JZ_MJZ_ZDJL b \n" +
            "WHERE\n" +
            "b.jzxh = a.jzxh \n" +
            "AND b.jzlb = a.jzlb \n" +
            "AND b.yljgdm = a.yljgdm \n" +
            "LIMIT 1 \n" +
            ") AS zdmc,\n" +
            "CAST( ' ' AS CHAR ) AS gzhcurl,\n" +
            "CAST( ' ' AS CHAR ) AS bhszmkurl,\n" +
            "CAST( 'gjzb,' AS CHAR ) 'show',\n" +
            "ifnull(\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jy_bgzb jysl \n" +
            "WHERE\n" +
            "hz.jzxh = jysl.jzxh \n" +
            "AND hz.jzlb = jysl.jzlb \n" +
            "AND hz.hzid = jysl.hzid \n" +
            "AND hz.yljgdm = jysl.yljgdm \n" +
            "AND jysl.bgzt IN ( '06', '07' ) \n" +
            "AND jysl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            "),\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jy_wswbgzb jysl \n" +
            "WHERE\n" +
            "a.jzxh = jysl.jzxh \n" +
            "AND a.jzlb = jysl.jzlb \n" +
            "AND a.hzid = jysl.hzid \n" +
            "AND a.yljgdm = jysl.yljgdm \n" +
            "AND jysl.bgzt IN ( '06', '07' ) \n" +
            "AND jysl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") \n" +
            ") AS jysl,\n" +
            "ifnull(\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "mzjcsl jcsl \n" +
            "WHERE\n" +
            "a.yljgdm = jcsl.yljgdm \n" +
            "AND a.jzxh = jcsl.jzxh \n" +
            "AND a.jzlb = jcsl.jzlb \n" +
            "-- \tAND jcsl.bgzt IN ( '06', '07' ) \n" +
            "-- \tAND jcsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            "),\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jcbl_bgzb jcsl \n" +
            "WHERE\n" +
            "a.jzxh = jcsl.jzxh \n" +
            "AND a.jzlb = jcsl.jzlb \n" +
            "AND a.yljgdm = jcsl.yljgdm \n" +
            "AND a.hzid = jcsl.hzid \n" +
            "AND jcsl.bgzt IN ( '06', '07' ) \n" +
            "AND jcsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") \n" +
            ") AS jcsl,\n" +
            "ifnull(\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "yz_mjz_fypcfxx cfsl \n" +
            "WHERE\n" +
            "a.jzxh = cfsl.jzxh \n" +
            "AND a.jzlb = cfsl.jzlb \n" +
            "AND a.hzid = cfsl.hzid \n" +
            "AND a.yljgdm = cfsl.yljgdm \n" +
            "AND cfsl.cfjlzt = '0' \n" +
            "AND cfsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            "),\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "yz_mjz_ypcfxx cfsl \n" +
            "WHERE\n" +
            "a.jzxh = cfsl.jzxh \n" +
            "AND a.jzlb = cfsl.jzlb \n" +
            "AND a.hzid = cfsl.hzid \n" +
            "AND a.yljgdm = cfsl.yljgdm \n" +
            "AND cfsl.cfjlzt = '0' \n" +
            "AND cfsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") \n" +
            ") AS cfsl,\n" +
            "0 AS yzsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "mzgmsl gmsl \n" +
            "WHERE\n" +
            "a.jzxh = gmsl.jzxh \n" +
            "AND a.jzlb = gmsl.jzlb \n" +
            "AND a.yljgdm = gmsl.yljgdm -- and gmsl.isdelete='0'\n" +
            "\n" +
            "LIMIT 1 \n" +
            ") AS gmsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jz_mjz_zdjl zdsl \n" +
            "WHERE\n" +
            "a.jzxh = zdsl.jzxh \n" +
            "AND a.jzlb = zdsl.jzlb \n" +
            "AND a.hzid = zdsl.hzid \n" +
            "AND a.yljgdm = zdsl.yljgdm \n" +
            "AND zdsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS zdsl,\n" +
            "0 AS blsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "ss_ssjl sssl \n" +
            "WHERE\n" +
            "a.jzxh = sssl.jzxh \n" +
            "AND a.jzlb = sssl.jzlb \n" +
            "AND a.hzid = sssl.hzid \n" +
            "AND a.yljgdm = sssl.yljgdm \n" +
            "AND sssl.sszt <> '5' \n" +
            "AND sssl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS sssl,\n" +
            "0 AS tzsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "yp_mz_fyjl fysl \n" +
            "WHERE\n" +
            "a.jzxh = fysl.jzxh \n" +
            "AND a.hzid = fysl.hzid \n" +
            "AND a.yljgdm = fysl.yljgdm \n" +
            "AND fysl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS fyjlsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "fy_mjz_ghsfjl sfjlsl \n" +
            "WHERE\n" +
            "a.jzxh = sfjlsl.jzxh \n" +
            "AND a.jzlb = sfjlsl.jzlb \n" +
            "AND a.hzid = sfjlsl.hzid \n" +
            "AND a.yljgdm = sfjlsl.yljgdm \n" +
            "AND sfjlsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS sfjlsl,\n" +
            "0 AS tjbgsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "sx_sqdjl sxjl \n" +
            "WHERE\n" +
            "a.jzxh = sxjl.jzxh \n" +
            "AND a.jzlb = sxjl.jzlb \n" +
            "AND a.hzid = sxjl.hzid \n" +
            "AND a.yljgdm = sxjl.yljgdm \n" +
            "AND sxjl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") sxsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "sx_sxjl sxjlsl \n" +
            "WHERE\n" +
            "a.jzxh = sxjlsl.jzxh \n" +
            "AND a.jzlb = sxjlsl.jzlb \n" +
            "AND a.hzid = sxjlsl.hzid \n" +
            "AND a.yljgdm = sxjlsl.yljgdm \n" +
            "AND sxjlsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") sxjlsl,\n" +
            "0 AS gzhcsl \n" +
            "FROM\n" +
            "HZ_JZJBXX hz\n" +
            "INNER JOIN jz_mjz_jzjl a ON a.jzxh = hz.jzxh \n" +
            "AND a.jzlb = hz.jzlb \n" +
            "AND a.yljgdm = hz.yljgdm \n" +
            "AND a.ghjlzt IN ( '0', '2' ) \n" +
            "AND a.isdelete = '0' \n" +
            "AND a.ghsj >= TIMESTAMP '1900-01-01 00:00:00' \n" +
            "AND a.ghsj <= TIMESTAMP '2024-05-10 23:59:59' \n" +
            "WHERE\n" +
            "-- hz.hzid IN ( '6900538417', '1110311951', '1110507269', '1110530535', '6900712627', '4010303723' ) \n" +
            "hz.hzid in ('70360152')\n" +
            "UNION ALL\n" +
            "SELECT\n" +
            "CAST( ' ' AS CHAR ) opentype,\n" +
            "-- -- 打开方式 新窗口:_blank 内嵌:iframe_page\n" +
            "CAST( ' ' AS CHAR ) url,\n" +
            "-- -- 链接\n" +
            "a.jzxh,\n" +
            "-- -- 就诊序号\n" +
            "a.hzid,\n" +
            "-- -- 患者id\n" +
            "a.yljgdm,\n" +
            "-- -- 医疗机构代码\n" +
            "a.yljgmc,\n" +
            "-- -- 医疗机构名称\n" +
            "ifnull( a.zyysdm, ifnull( a.zzysdm, zrysdm ) ) ysdm,\n" +
            "-- -- 医生代码\n" +
            "ifnull( a.zyysmc, ifnull( a.zzysmc, zrysmc ) ) ysmc,\n" +
            "-- -- 医生名称\n" +
            "'3' jzlb,\n" +
            "-- -- 就诊类别\n" +
            "'住院' jzlbzw,\n" +
            "-- -- 就诊类别名称\n" +
            "hz.hzsfzh hzsfzh,\n" +
            "-- -- 患者身份证\n" +
            "a.dqksdm ksdm,\n" +
            "-- -- 当前科室代码\n" +
            "a.dqksmc ksmc,\n" +
            "-- -- 当前科室名称\n" +
            "a.dqbqdm bqdm,\n" +
            "-- -- 当前病区代码\n" +
            "a.dqbqmc bqmc,\n" +
            "-- -- 当前病区名称\n" +
            "a.cwh cwh,\n" +
            "-- -- 床位号\n" +
            "ifnull( a.rysj, a.rqsj ) rysj,\n" +
            "-- -- 入区时间\n" +
            "ifnull( ifnull( a.cysj, a.cqsj ), NOW( ) ) AS cysj,\n" +
            "-- -- 出区时间\n" +
            "TIMESTAMPDIFF( DAY, a.rqsj, a.cqsj ) + 1 AS zyts,\n" +
            "-- -- 住院天数\n" +
            "ifnull (\n" +
            "(\n" +
            "SELECT\n" +
            "b.ynzddm \n" +
            "FROM\n" +
            "JZ_ZY_ZDJL b \n" +
            "WHERE\n" +
            "b.jzxh = a.jzxh \n" +
            "AND b.yljgdm = a.yljgdm \n" +
            "AND b.hzid = a.hzid \n" +
            "AND b.zdlxdm = '0' \n" +
            "AND b.zdlbdm = '02' \n" +
            "LIMIT 1 \n" +
            "),\n" +
            "(\n" +
            "SELECT\n" +
            "b.ynzddm \n" +
            "FROM\n" +
            "JZ_ZY_ZDJL b \n" +
            "WHERE\n" +
            "b.jzxh = a.jzxh \n" +
            "AND b.yljgdm = a.yljgdm \n" +
            "AND b.hzid = a.hzid \n" +
            "AND b.zdlxdm = '0' \n" +
            "AND b.zdlbdm = '03' \n" +
            "LIMIT 1 \n" +
            ") \n" +
            ") AS zddm,\n" +
            "ifnull(\n" +
            "(\n" +
            "SELECT\n" +
            "b.ynzdmc \n" +
            "FROM\n" +
            "JZ_ZY_ZDJL b \n" +
            "WHERE\n" +
            "b.jzxh = a.jzxh \n" +
            "AND b.yljgdm = a.yljgdm \n" +
            "AND b.hzid = a.hzid \n" +
            "AND b.zdlxdm = '0' \n" +
            "AND b.zdlbdm = '02' \n" +
            "LIMIT 1 \n" +
            "),\n" +
            "(\n" +
            "SELECT\n" +
            "b.ynzdmc \n" +
            "FROM\n" +
            "JZ_ZY_ZDJL b \n" +
            "WHERE\n" +
            "b.jzxh = a.jzxh \n" +
            "AND b.yljgdm = a.yljgdm \n" +
            "AND b.hzid = a.hzid \n" +
            "AND b.zdlxdm = '0' \n" +
            "AND b.zdlbdm = '03' \n" +
            "LIMIT 1 \n" +
            ") \n" +
            ") AS zdmc,\n" +
            "CAST( ' ' AS CHAR ) AS gzhcurl,\n" +
            "CAST( ' ' AS CHAR ) AS bhszmkurl,\n" +
            "CAST( 'gjzb,' AS CHAR ) 'show',\n" +
            "ifnull(\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jy_bgzb jysl \n" +
            "WHERE\n" +
            "a.jzxh = jysl.jzxh \n" +
            "AND jysl.jzlb = '3' \n" +
            "AND a.hzid = jysl.hzid \n" +
            "AND a.yljgdm = jysl.yljgdm \n" +
            "AND jysl.bgzt IN ( '06', '07' ) \n" +
            "AND jysl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            "),\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jy_wswbgzb jysl \n" +
            "WHERE\n" +
            "a.jzxh = jysl.jzxh \n" +
            "AND jysl.jzlb = '3' \n" +
            "AND a.hzid = jysl.hzid \n" +
            "AND a.yljgdm = jysl.yljgdm \n" +
            "AND jysl.bgzt IN ( '06', '07' ) \n" +
            "AND jysl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") \n" +
            ") AS jysl,\n" +
            "ifnull(\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "zyjcsl   jcsl \n" +
            "WHERE\n" +
            "a.yljgdm = jcsl.yljgdm \n" +
            "AND a.jzxh = jcsl.jzxh \n" +
            "LIMIT 1 \n" +
            "),\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jcbl_bgzb jcsl \n" +
            "WHERE\n" +
            "a.jzxh = jcsl.jzxh \n" +
            "AND a.hzid = jcsl.hzid \n" +
            "AND a.yljgdm = jcsl.yljgdm \n" +
            "AND jcsl.jzlb = '3' \n" +
            "AND jcsl.bgzt IN ( '06', '07' ) \n" +
            "AND jcsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") \n" +
            ") AS jcsl,\n" +
            "0 AS cfsl,\n" +
            "ifnull(\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "yz_zy_ypyzxx yzsl \n" +
            "WHERE\n" +
            "a.jzxh = yzsl.jzxh \n" +
            "AND a.hzid = yzsl.hzid \n" +
            "AND a.yljgdm = yzsl.yljgdm \n" +
            "AND yzsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            "),\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "yz_zy_fypyzxx yzsl \n" +
            "WHERE\n" +
            "a.jzxh = yzsl.jzxh \n" +
            "AND a.hzid = yzsl.hzid \n" +
            "AND a.yljgdm = yzsl.yljgdm \n" +
            "AND yzsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") \n" +
            ") AS yzsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "zygmsl gmsl \n" +
            "WHERE\n" +
            "a.jzxh = gmsl.jzxh \n" +
            "AND a.yljgdm = gmsl.yljgdm \n" +
            "AND gmsl.jzlb = '3' \n" +
            "LIMIT 1 \n" +
            ") AS gmsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jz_zy_zdjl zdsl \n" +
            "WHERE\n" +
            "a.jzxh = zdsl.jzxh \n" +
            "AND a.hzid = zdsl.hzid \n" +
            "AND a.yljgdm = zdsl.yljgdm \n" +
            "AND zdsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS zdsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "bl_blwdjl blsl \n" +
            "WHERE\n" +
            "a.jzxh = blsl.jzxh \n" +
            "AND a.hzid = blsl.hzid \n" +
            "AND a.yljgdm = blsl.yljgdm \n" +
            "AND blsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS blsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "ss_ssjl sssl \n" +
            "WHERE\n" +
            "a.jzxh = sssl.jzxh \n" +
            "AND a.hzid = sssl.hzid \n" +
            "AND a.yljgdm = sssl.yljgdm \n" +
            "AND sssl.jzlb = '3' \n" +
            "AND sssl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS sssl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "hl_tz_tzxx tzsl \n" +
            "WHERE\n" +
            "a.jzxh = tzsl.jzxh \n" +
            "AND a.hzid = tzsl.hzid \n" +
            "AND a.yljgdm = tzsl.yljgdm \n" +
            "AND tzsl.jzlb = '3' \n" +
            "AND tzsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS tzsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "yp_mz_fyjl fysl \n" +
            "WHERE\n" +
            "a.jzxh = fysl.jzxh \n" +
            "AND a.hzid = fysl.hzid \n" +
            "AND a.yljgdm = fysl.yljgdm \n" +
            "AND fysl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS fyjlsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "fy_zy_jsjl sfjlsl \n" +
            "WHERE\n" +
            "a.jzxh = sfjlsl.jzxh \n" +
            "AND a.hzid = sfjlsl.hzid \n" +
            "AND a.yljgdm = sfjlsl.yljgdm \n" +
            "AND sfjlsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS sfjlsl,\n" +
            "0 AS tjbgsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "sx_sqdjl sxjl \n" +
            "WHERE\n" +
            "a.jzxh = sxjl.jzxh \n" +
            "AND a.hzid = sxjl.hzid \n" +
            "AND a.yljgdm = sxjl.yljgdm \n" +
            "AND sxjl.jzlb = '3' \n" +
            "AND sxjl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") sxsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "sx_sxjl sxjlsl \n" +
            "WHERE\n" +
            "a.jzxh = sxjlsl.jzxh \n" +
            "AND a.hzid = sxjlsl.hzid \n" +
            "AND a.yljgdm = sxjlsl.yljgdm \n" +
            "AND sxjlsl.jzlb = '3' \n" +
            "AND sxjlsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") sxjlsl,\n" +
            "0 AS gzhcsl \n" +
            "FROM\n" +
            "JZ_ZY_ZYJL a\n" +
            "INNER JOIN HZ_JZJBXX hz ON a.jzxh = hz.jzxh \n" +
            "AND a.yljgdm = hz.yljgdm \n" +
            "AND a.hzid = hz.hzid \n" +
            "AND hz.jzlb = '3' \n" +
            "-- AND hz.hzid IN ( '6900538417', '1110311951', '1110507269', '1110530535', '6900712627', '4010303723' ) \n" +
            "and  hz.hzid in ('70360152')\n" +
            "\n" +
            "WHERE\n" +
            "a.brzyzt <> '9' \n" +
            "AND a.isdelete = '0' \n" +
            "AND ( ( a.rysj >= TIMESTAMP '1900-01-01 00:00:00' AND a.rysj <= TIMESTAMP '2024-05-10 23:59:59' ) OR a.brzyzt = '1' ) UNION ALL\n" +
            "SELECT\n" +
            "CAST( 'iframe_page' AS CHAR ) opentype,\n" +
            "-- -- 打开方式 新窗口:_blank 内嵌:iframe_page\n" +
            "CAST( b.tjbgnr AS CHAR ) url,\n" +
            "-- -- 链接\n" +
            "a.tjxh jzxh,\n" +
            "-- -- 就诊序号\n" +
            "a.tjrybs hzid,\n" +
            "-- -- 患者id\n" +
            "a.yljgdm,\n" +
            "-- -- 医疗机构代码\n" +
            "a.yljgmc,\n" +
            "-- -- 医疗机构名称\n" +
            "CAST( ' ' AS CHAR ) ysdm,\n" +
            "-- -- 医生代码\n" +
            "CAST( ' ' AS CHAR ) ysmc,\n" +
            "-- -- 医生名称\n" +
            "'4' jzlb,\n" +
            "-- -- 就诊类别\n" +
            "'体检' jzlbzw,\n" +
            "-- -- 就诊类别名称\n" +
            "CAST( ' ' AS CHAR ) hzsfzh,\n" +
            "-- -- 患者身份证\n" +
            "a.tjtcmc ksdm,\n" +
            "-- -- 当前科室代码\n" +
            "'体检报告' ksmc,\n" +
            "-- -- 当前科室名称\n" +
            "CAST( ' ' AS CHAR ) bqdm,\n" +
            "-- -- 当前病区代码\n" +
            "CAST( ' ' AS CHAR ) bqmc,\n" +
            "-- -- 当前病区名称\n" +
            "CAST( ' ' AS CHAR ) cwh,\n" +
            "-- -- 床位号\n" +
            "a.tjkssj rysj,\n" +
            "-- -- 入区时间\n" +
            "ifnull( a.tjjssj, NOW( ) ) cysj,\n" +
            "-- -- 出区时间\n" +
            "0 zyts,\n" +
            "-- -- 住院天数\n" +
            "CAST( ' ' AS CHAR ) zddm,\n" +
            "-- -- 主诊断代码\n" +
            "CAST( ' ' AS CHAR ) zdmc,\n" +
            "-- -- 主诊断名称\n" +
            "CAST( ' ' AS CHAR ) AS gzhcurl,\n" +
            "CAST( ' ' AS CHAR ) AS bhszmkurl,\n" +
            "CAST( 'gjzb,' AS CHAR ) 'show',\n" +
            "ifnull(\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jy_bgzb jysl \n" +
            "WHERE\n" +
            "d.jzxh = jysl.jzxh \n" +
            "AND d.hzid = jysl.hzid \n" +
            "AND d.yljgdm = jysl.yljgdm \n" +
            "AND jysl.jzlb = 4 \n" +
            "AND jysl.bgzt IN ( '06', '07' ) \n" +
            "AND jysl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            "),\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jy_wswbgzb jysl \n" +
            "WHERE\n" +
            "d.jzxh = jysl.jzxh \n" +
            "AND d.hzid = jysl.hzid \n" +
            "AND d.yljgdm = jysl.yljgdm \n" +
            "AND jysl.jzlb = 4 \n" +
            "AND jysl.bgzt IN ( '06', '07' ) \n" +
            "AND jysl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") \n" +
            ") AS jysl,\n" +
            "ifnull(\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jc_bgzb jcsl \n" +
            "WHERE\n" +
            "d.yljgdm = jcsl.yljgdm \n" +
            "AND d.jzxh = jcsl.jzxh \n" +
            "AND d.hzid = jcsl.hzid \n" +
            "AND jcsl.jzlb = '4' \n" +
            "AND jcsl.bgzt IN ( '06', '07' ) \n" +
            "AND jcsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            "),\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jcbl_bgzb jcsl \n" +
            "WHERE\n" +
            "d.jzxh = jcsl.jzxh \n" +
            "AND d.hzid = jcsl.hzid \n" +
            "AND d.yljgdm = jcsl.yljgdm \n" +
            "AND jcsl.jzlb = '4' \n" +
            "AND jcsl.bgzt IN ( '06', '07' ) \n" +
            "AND jcsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") \n" +
            ") AS jcsl,\n" +
            "0 AS cfsl,\n" +
            "0 AS yzsl,\n" +
            "0 AS gmsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "jz_mjz_zdjl zdsl \n" +
            "WHERE\n" +
            "d.jzxh = zdsl.jzxh \n" +
            "AND d.hzid = zdsl.hzid \n" +
            "AND d.yljgdm = zdsl.yljgdm \n" +
            "AND zdsl.jzlb = '4' \n" +
            "AND zdsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS zdsl,\n" +
            "0 AS blsl,\n" +
            "0 AS sssl,\n" +
            "0 AS tzsl,\n" +
            "0 AS fyjlsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "fy_mjz_ghsfjl sfjlsl \n" +
            "WHERE\n" +
            "d.jzxh = sfjlsl.jzxh \n" +
            "AND d.hzid = sfjlsl.hzid \n" +
            "AND d.yljgdm = sfjlsl.yljgdm \n" +
            "AND sfjlsl.jzlb = '4' \n" +
            "AND sfjlsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS sfjlsl,\n" +
            "(\n" +
            "SELECT\n" +
            "1 \n" +
            "FROM\n" +
            "tj_tjdjxx tjbgsl \n" +
            "WHERE\n" +
            "d.jzxh = tjbgsl.tjxh \n" +
            "AND d.yljgdm = tjbgsl.yljgdm \n" +
            "AND tjbgsl.isdelete = '0' \n" +
            "LIMIT 1 \n" +
            ") AS tjbgsl,\n" +
            "0 sxsl,\n" +
            "0 sxjlsl,\n" +
            "0 AS gzhcsl \n" +
            "FROM\n" +
            "TJ_TJDJXX a\n" +
            "LEFT JOIN TJ_TJBGWD b ON a.tjbh = b.tjbh \n" +
            "AND a.tjxh = b.tjxh \n" +
            "AND b.isdelete = '0'\n" +
            "INNER JOIN HZ_JZJBXX d ON a.tjxh = d.jzxh \n" +
            "AND a.yljgdm = d.yljgdm \n" +
            "AND d.jzlb = '4' \n" +
            "-- and d.hzid in('6900538417', '1110311951' , '1110507269' , '1110530535', '6900712627', '4010303723')\n" +
            "AND d.hzid IN ( '70360152' ) \n" +
            "AND d.jlzt = '1' \n" +
            "AND d.isdelete = '0' \n" +
            "WHERE\n" +
            "a.isdelete = '0' \n" +
            "AND a.tjkssj >= TIMESTAMP ( '1900-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss' ) \n" +
            "AND a.tjjssj <= TIMESTAMP ( '2024-05-10 23:59:59', 'yyyy-mm-dd hh24:mi:ss' ) \n" +
            ") \n" +
            "\n" +
            "\n" +
            "SELECT\n" +
            "opentype,\n" +
            "url,\n" +
            "jzxh,\n" +
            "hzid,\n" +
            "yljgdm,\n" +
            "yljgmc,\n" +
            "ysdm,\n" +
            "ysmc,\n" +
            "jzlb,\n" +
            "jzlbzw,\n" +
            "hzsfzh,\n" +
            "ksdm,\n" +
            "ksmc,\n" +
            "bqdm,\n" +
            "bqmc,\n" +
            "cwh,\n" +
            "rysj,\n" +
            "cysj,\n" +
            "zyts,\n" +
            "zddm,\n" +
            "zdmc,\n" +
            "CONCAT( '/dataView/tybg.html?moduleid=gzhc&jzxh=', jzxh, '&jzlb=', jzlb ) gzhcurl,\n" +
            "CONCAT( 'http://localhost:9038/#/bhgl/bhssjk?jzxh=', a.jzxh, '&usercode=@usercode&jzlb=', jzlb ) bhszmkurl,\n" +
            "CONCAT(\n" +
            "a.SHOW,\n" +
            "( CASE WHEN jysl > 0 THEN 'jybg,' ELSE '' END ),\n" +
            "( CASE WHEN jcsl > 0 THEN 'jcbg,' ELSE '' END ),\n" +
            "( CASE WHEN cfsl > 0 THEN 'mzcf,' ELSE '' END ),\n" +
            "( CASE WHEN yzsl > 0 THEN 'zyyz,' ELSE '' END ),\n" +
            "( CASE WHEN gmsl > 0 THEN 'gmy,' ELSE '' END ),\n" +
            "( CASE WHEN zdsl > 0 THEN 'lczd,' ELSE '' END ),\n" +
            "( CASE WHEN blsl > 0 THEN 'blzl,' ELSE '' END ),\n" +
            "( CASE WHEN sssl > 0 THEN 'ssjl,' ELSE '' END ),\n" +
            "( CASE WHEN tzsl > 0 THEN 'tzxx,' ELSE '' END ),\n" +
            "( CASE WHEN fyjlsl > 0 THEN 'fyjl,' ELSE '' END ),\n" +
            "( CASE WHEN sfjlsl > 0 THEN 'sfjl,' ELSE '' END ),\n" +
            "( CASE WHEN tjbgsl > 0 THEN 'tjbg,' ELSE '' END ),\n" +
            "( CASE WHEN sxjlsl > 0 THEN 'sxjl,' ELSE '' END ),\n" +
            "( CASE WHEN sxsl > 0 THEN 'sx,' ELSE '' END ),\n" +
            "( CASE WHEN gzhcsl > 0 THEN 'gzhc,' ELSE '' END ),\n" +
            "'bhszmk,' \n" +
            ") 'show' \n" +
            "FROM\n" +
            "cte_all a \n" +
            "ORDER BY\n" +
            "rysj DESC,\n" +
            "jzlb ASC ";
    public static void main(String[] args) throws Exception {
        parseArgs(args);
        List<String> data = new TestPreparedOnce1().getTestData();
        System.out.println(data.size());

        String driver = "com.mysql.jdbc.Driver";
        String url = String.format("jdbc:mysql://172.16.10.101:3390/hdw?useServerPrepStmts=true&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSqlLimit=256000000&prepStmtCacheSize=2500");

        try {
            Class.forName(driver);


            Connection connection = DriverManager.getConnection(url, USER, PASSWORD);
            
            Statement statement = connection.prepareStatement(SQL);





            System.exit(0);
        } catch (Exception e) {
            LOGGER.error("error happen here,", e);
        }
    }
    static  class QPSThread extends Thread {
        public void run() {
            try {
                while (true) {
                    int last = TOTAL_SIZE.get();
                    Thread.sleep(1000);
                    LOGGER.info("QPS is:" + (TOTAL_SIZE.get() - last));
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    private static void query(Connection connection, List<String> data, int index) throws Exception {
        int start = data.size() / THREAD_NUM * (index - 1);
        int end = data.size() / THREAD_NUM * index;
        try (PreparedStatement ps =  connection.prepareStatement(SQL)) {
            for (int j = 0; j < 30; j++) {
                for (int i = start; i < end; i++) {
                    ps.setString(1, data.get(i));
                    ResultSet resultSet = ps.executeQuery();
                    while (resultSet.next()) {

                    }
                    TOTAL_SIZE.addAndGet(1);
                }
            }
        }
    }

    private List<String> getTestData() throws IOException {
        InputStream fileInPath = this.getClass().getResourceAsStream("/data.txt");
        List<String> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader((InputStream) fileInPath))) {
            String line;
            while ((line = br.readLine()) != null) {
                data.add(line);
            }
        }
        return data;
    }

    private static void parseArgs(String [] args) {
        if (args.length == 5) {
            THREAD_NUM = Integer.parseInt(args[0]);
            IP_PORT = args[1];
            USER = args[2];
            PASSWORD = args[3];
            DB_NAME = args[4];
        } else {
            LOGGER.error("please input 5 args: THREAD_NUM IP_PORT USER PASSWORD DB_NAME ");
            System.exit(1);
        }
    }
}
