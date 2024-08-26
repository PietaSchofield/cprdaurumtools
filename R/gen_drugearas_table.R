#' generate a drug exposures table
#'
#'
#' @export
gen_drugeras_table <- function(dbf,tabname="drug_codes",
                               dosetab="dose_codes",newtab="drug_eras",
                               substancefield="atc",db=F){
  if(db){
    tabname <- "drug_codes"
    newtab <- "drug_eras"
    dosetab <- "dose_codes"
    substancefield="atc"
    dbf <- dbif
  }

  drugcode <- get_table(dbf=dbf, sqlstr=str_c("SELECT * FROM drug_codes"))
  drugcode %>% plib::display_data(disp=T,buttons=T)


  make_ranked_issues_sql <- stringr::str_c("
  CREATE TABLE
    issue_grouping
  AS (
  WITH ranked_issues AS (
    SELECT DISTINCT
      d.patid,
      d.prodcodeid,
      d.dosageid,
      CAST(d.issuedate AS DATE) issuedate,
      ROW_NUMBER() OVER (PARTITION BY d.patid, d.prodcodeid, d.dosageid ORDER BY issuedate) AS rn
    FROM 
      drug_issues AS d
    INNER JOIN
      ",tabname," AS e
      ON e.prodcodeid=d.prodcodeid
      ),
  date_differences AS (
    SELECT
      patid,
      prodcodeid,
      dosageid,
      issuedate,
      rn,
      LAG(issuedate,1) OVER (PARTITION BY patid, prodcodeid, dosageid ORDER BY issuedate) AS previssue
    FROM
      ranked_issues)
  SELECT 
    patid,
    prodcodeid,
    dosageid,
    issuedate,
    rn,
    previssue,
    CASE 
      WHEN previssue IS NULL THEN 0
      ELSE DATE_DIFF('MONTH',previssue,issuedate)
    END AS monthdiff,
    SUM(CASE WHEN DATE_DIFF('MONTH',previssue,issuedate) >3 THEN 1 ELSE 0 END) OVER
        (PARTITION BY patid, prodcodeid, dosageid ORDER BY issuedate) AS period_group
  FROM 
    date_differences)")


  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  if("issue_grouping"%in%dbListTables(dbi)) dbExecute(dbi,"DROP TABLE issue_grouping")
  dbExecute(dbi,make_ranked_issues_sql)
  dbDisconnect(dbi)

  group_sql <- stringr::str_c("
  CREATE TABLE
    ",newtab,"
  AS(
  SELECT
    ig.patid AS 'patient.id',
    MIN(ig.issuedate) AS 'drug.era.start.date',
    MAX(ig.issuedate) AS 'drug.era.end.date',
    dc.atc AS 'atc.code',
    dc.moiety AS 'drug.name',
    CASE WHEN dd.dose_unit LIKE 'MG' THEN dd.daily_dose ELSE NULL END AS 'daily.dose' 
  FROM
    issue_grouping AS ig
  INNER JOIN
    ",tabname," AS dc
    ON dc.prodcodeid=ig.prodcodeid
  INNER JOIN
    ",dosetab," AS dd
    ON dd.dosageid=ig.dosageid
  GROUP BY
    ig.patid,
    dc.atc,
    dc.moiety,
    dc.termfromemis,
    dd.dosage_text,
    dd.daily_dose,
    dd.dose_unit,
    period_group
  HAVING 
    COUNT(*) >= 1) ")

  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  if(newtab %in% dbListTables(dbi)) dbExecute(dbi,str_c("DROP TABLE ",newtab," ;"))
  dbExecute(dbi,group_sql) 
  dbDisconnect(dbi)

  return(get_table(dbf,paste0("SELECT * FROM ",newtab,";")))
}




