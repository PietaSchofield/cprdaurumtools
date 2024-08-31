# get observations batch
#'
#' get the drugissue records and convert some fields to useful field types
#'
#' @param bdir the name of the batch subdirectory
#' @param odir the name of the output directory
#' @param olist the list of observation codes
#' @param bpp BiocParallal Multicore Parameters
#'
#'
#' Pass a table of covariate codes and generate covariates table
#' @import magrittr
#' @export
load_probs <- function( pddir,dbf,ow=F,db=F,tab_name="problems",add=F,
    selvars=c("patid","obsid","parentprobobsid","probenddate","expduration","lastrevdate",
              "parentprobrelid","probstatusid")){
  if(F){
    pddir <- apath
    dbf <- sadb
    ow <- F
    db <- F
    add <- T
    tab_name <- "problems"
    selvars <- c("patid","obsid","parentprobobsid","probenddate","expduration","lastrevdate",
              "parentprobrelid","probstatusid")
  }
  if(ow && add){
    stop("Error overwrite and append both true\n")
    return()
  }
  dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
  tabs <- dbListTables(dbi)
  dbDisconnect(dbi)
  nrec <- 0
  if(!tab_name%in%tabs || ow || add){
    profiles <- list.files(pddir,pattern="Prob.*txt$",full=T,recur=T)
    if(tab_name%in%tabs && ow){
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      duckdb::dbExecute(dbi,paste0("DROP TABLE ",tab_name,";"))
      dbDisconnect(dbi)
    }
    nrec <- lapply(profiles,function(fn){
      dat <- readr::read_tsv(fn,col_types=readr::cols(.default=readr::col_character())) %>%
        dplyr::select(dplyr::all_of(selvars)) %>%
        dplyr::mutate(probeddate = format(lubridate::dmy(probenddate)),
                      lastrevdate = format(lubridate::dmy(lastrevdate)))
      dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
      duckdb::dbWriteTable(dbi,tab_name,dat,append=T)
      dbDisconnect(dbi)
      nr <- dat %>% nrow()
      cat(paste0(basename(fn),": ",nr," records loaded\n"))
      rm(dat)
      gc()
      return(nr)
    })
    ext <- "new records"
  }else{
    dbi <- duckdb::dbConnect(duckdb::duckdb(),dbf)
    nrec <- dbGetQuery(dbi,paste0("SELECT COUNT(*) FROM ",tab_name,";"))
    dbDisconnect(dbi)
    ext <- "records exist"
  }
  trec <- sum(unlist(nrec))
  return(cat(paste0(trec," ",ext,"\n")))
}
