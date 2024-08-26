#' generate conditions table
#'
#' @export
gen_conditions_table <- function(dbf,
     apctab="hes_diagnosis_hosp", 
     epitab="hes_diagnosis_epi",
     onstab="death_patient",
     obstab="observations",
     db=F){

  if(db){
    db <- F
    apctab <- "hes_diagnosis_hosp"
    epitab <- "hes_diagnosis_epi"
    onstab <- "death_patient"
    obstab <- "observations"
    dbf <- file.path(.locData,"batches","t2dd_etl_b1.duckdb")
  }

  sqlstr <- str_c(" 
    SELECT DISTINCT
      patid AS person_id,
          FIRST(admidate) OVER (PARTITION BY patid, icd ORDER BY admidate) AS condition_start_date,
          icd AS icd10_code,
          '6' AS condition_type
    FROM ",apctab,"
    ORDER BY patid, icd")
    
  apc_data <- cprdaurumtools::get_table(dbf=dbf,sqlstr=sqlstr)

  sqlstr <- str_c("
    SELECT DISTINCT *
    FROM ",onstab,"
    ORDER BY patid")
    
  ons_data <- cprdaurumtools::get_table(dbf=dbf,sqlstr=sqlstr) %>%
       select(patid,dod,cause,cause1,cause2,cause3,cause4,cause5,cause6,cause7,cause8,cause9,cause10,
              cause11,cause12,cause13,cause14,cause15) %>%
       pivot_longer(-c(patid,dod),names_to="codetype",values_to="icd10_code") %>% na.omit() %>%
       mutate(condition_type=ifelse(codetype=="cause",'1','2'),sourcetab="ons") %>%
       rename(condition_start_date=dod,person_id=patid) %>%
       select(person_id,condition_start_date,icd10_code,condition_type) 

  con_data <- bind_rows(apc_data,ons_data) %>%
       arrange(person_id,condition_start_date,icd10_code,condition_type) 

  names(con_data) <- gsub("_",".",names(con_data)) 
  return(con_data)
}
