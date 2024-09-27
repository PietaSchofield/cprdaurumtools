#' make a full database
#'
#' @export
make_fulldb <- function(dbFile,aPath,cPath,db=F,ow=F){
  if(db){
    dbFile <- file.path(.locData,"aurum_final.duckdb")
    aPath <- file.path(.locData,"aurum","extract")
    cPath <- file.path(.locDir,"refdata","aurum","202409_Lookups_CPRDAurum")
  }else{
   if(ow && file.exists(dbFile))
     unlink(dbFile)
  }
  if(file.exists(aPath)){
    cprdaurumtools::load_patients(pdir=aPath,dbf=dbFile)
    cprdaurumtools::load_obs(pddir=aPath,dbf=dbFile)
    cprdaurumtools::load_cons(pddir=aPath,dbf=dbFile)
    cprdaurumtools::load_referrals(pddir=aPath,dbf=dbFile)
    cprdaurumtools::load_practice(pddir=aPath,dbf=dbFile)
    cprdaurumtools::load_drugissues(pddir=aPath,dbf=dbFile)
    cprdaurumtools::load_probs(pddir=aPath,dbf=dbFile)
    cprdaurumtools::load_staff(pddir=aPath,dbf=dbFile)
    cprdaurumtools::load_cprdfiles(pddir=cPath,dbf=dbFile)
  }
  return(dbFile)
}
