#' generate conditions table
#'
#' @export
gen_conditions_table <- function(dbf,
     apctab="hes_diagnosis_hosp", 
     epitab="hes_diagnosis_epi",
     onstab="death_patient",
     obstab="observations"){
     sqlstr <- str_c(" 
        SELECT DISTINCT
          patid AS personid,
          FIRST(admidate) OVER (PARTITION BY patid, icd ORDER BY admidate) AS conditionstartdate,
          icd AS code,
          '6' AS conditiontype,
          'hosp' AS sourcetab
        FROM ",apctab,"
        ORDER BY patid, icd")
     apcdata <- get_table(dbf=dbf,sqlstr=sqlstr) 
     sqlstr <- str_c(" 
        SELECT DISTINCT
          patid AS personid,
          FIRST(epistart) OVER (PARTITION BY patid, icd ORDER BY epistart) AS conditionstartdate,
          icd AS code,
          '5' AS conditiontype,
          'epi' AS sourcetab
        FROM ",epitab,"
        ORDER BY patid, icd")
     epidata <- get_table(dbf=dbf,sqlstr=sqlstr) 
     sqlstr <- str_c(" 
        SELECT DISTINCT *
          FROM ",onstab,"
        ORDER BY patid")
     onsdata <- get_table(dbf=dbf,sqlstr=sqlstr) %>%
       select(patid,dod,cause,cause1,cause2,cause3,cause4,cause5,cause6,cause7,cause8,cause9,cause10,
              cause11,cause12,cause13,cause14,cause15) %>%
       pivot_longer(-c(patid,dod),names_to="codetype",values_to="code") %>% na.omit() %>%
       mutate(conditiontype=ifelse(codetype=="cause",'1','2'),sourcetab="ons") %>%
       rename(conditionstartdate=dod,personid=patid) %>%
       select(personid,conditionstartdate,code,conditiontype,sourcetab) 
     condata <- bind_rows(apcdata,epidata,onsdata) %>%
       arrange(personid,conditionstartdate,code,conditiontype) %>%
       mutate(linkcode=gsub("[.]","",code))
     condata %>% select(personid,conditionstartdate,linkcode,conditiontype) %>%
       rename(icd10code=linkcode)
}
