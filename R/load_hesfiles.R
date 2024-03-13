#' load hes files
#'
#' get the hes files records and convert some fields to useful field types
#'
#' @param pddir directory containing hes data
#' @param dbf name of duckdb database
#' @param tad text adjustment to remove from file names when creating table name
#' @param pats a list of patient ids to extract from the hes data
#' @param db debug switch
#' @param ow overwrite an existing file
#'
#'
#' @export
load_hesfiles <- function(pddir,dbf,ow=F,db=F,tad,pats,pattern="[.]txt"){
  if(db){
    pddir <- hdir 
    dbf <- dbif
    tad <- "21_001631.*"
    pats <- patids
    pattern <- "hes_patient"
    ow <- F
  }
  hesfiles <- list.files(pddir,pattern=pattern,full=T)
  names(hesfiles) <- tolower(gsub(paste0("_",tad,"[.]txt"),"",basename(hesfiles)))
  hesfiles
  lapply(names(hesfiles),function(fn){
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    if(!fn%in%duckdb::dbListTables(dbi) || ow){
      dat <- readr::read_tsv(hesfiles[[fn]],col_types=readr::cols(.default=readr::col_character())) %>%
        as_tibble() %>% dplyr::filter(patid%in%pats)
      names(dat) <- tolower(names(dat))
      switch(fn,
        hes_acp={
          dat <- dat %>% dplyr::mutate(epistart=lubridate::dmy(epistart),
                                       epiend=lubridate::dmy(epiend),
                                       acpstar=lubridate::dmy(acpstar),
                                       acpend=lubridate::dmy(acpend))
        },
        hes_ccare={
           dat <- dat %>% dplyr::mutate(admidate=lubridate::dmy(admidate),
                                       discharged=lubridate::dmy(discharged),
                                       epistart=lubridate::dmy(epistart),
                                       epiend=lubridate::dmy(epiend),
                                       ccstartdate=lubridate::dmy(ccstartdate),
                                       ccdisrdydate=lubridate::dmy(ccdisrdydate))
        },
        hes_diagnosis_epi={
          dat <- dat %>% dplyr::mutate(epistart=lubridate::dmy(epistart),
                                       epiend=lubridate::dmy(epiend))
        },
        hes_diagnosis_hosp={
          dat <- dat %>% dplyr::mutate(admidate=lubridate::dmy(admidate),
                                       discharged=lubridate::dmy(discharged))
        },
        hes_episodes={
          dat <- dat %>% dplyr::mutate(admidate=lubridate::dmy(admidate),
                                       discharged=lubridate::dmy(discharged),
                                       epistart=lubridate::dmy(epistart),
                                       epiend=lubridate::dmy(epiend))
        },
        hes_hospital={
          dat <- dat %>% dplyr::mutate(admidate=lubridate::dmy(admidate),
                                       discharged=lubridate::dmy(discharged),
                                       elecdate=lubridate::dmy(elecdate))
        },
        hes_primary_diag_hosp={
          dat <- dat %>% dplyr::mutate(admidate=lubridate::dmy(admidate),
                                       discharged=lubridate::dmy(discharged))
        },
        hes_procedures_epi={
          dat <- dat %>% dplyr::mutate(admidate=lubridate::dmy(admidate),
                                       discharged=lubridate::dmy(discharged),
                                       epistart=lubridate::dmy(epistart),
                                       epiend=lubridate::dmy(epiend),
                                       evdate=lubridate::dmy(evdate))
        },
        hes_patient={
          dat <- dat
        }
      )
      duckdb::dbWriteTable(dbi,fn,dat,overwrite=T)
      fn
      duckdb::dbDisconnect(dbi)
      nr <- dat %>% nrow()
      ext <- "new records"
      rm(dat)
      gc()
    }else{
      nr <- dbGetQuery(dbi,paste0("SELECT COUNT(*) FROM ",fn))
      ext <- "records exist"
      duckdb::dbDisconnect(dbi)
    }
    cat(paste0(basename(fn),": ",nr," ",ext,"\n"))
  })
  return()
}
