## define some parameters
# choose cloud = TRUE if you run the script on AWS, otherwise leave as is
cloud = FALSE

# declare your backet name here
bucket_name = "econometricsatscale"

# decöare the local folder where HMDA_subsample.csv is stored
local_folder = "C:/Users/janni/Dropbox/university/research/Econometrics_at_Scale/Data/HMDA/"

# load some libraries
library('dplyr')
library('sparklyr')
library('ggplot2')

# initialize spark config and connect
conf <- spark_config() 
if(!cloud){
  sc <- spark_connect(master = "local", config = config, version = '2.4')
} else {
  sc <- spark_connect(master = "yarn-client", config = config, version = '2.4')
}


# load the dataset
if(!cloud){
  hmda_spark = spark_read_csv(sc, name = "hmda_spark", header= TRUE, delimiter = ",", overwrite = TRUE, 
                              path=paste(local_folder,"HMDA_subsample.csv", sep =""))
} else {
  # import the datasets, note that you may need to change the folder name
  data2007 = spark_read_csv(sc, name = "data_tbl2007", path = paste(bucket_name,"data/micro/hmda_2007_nationwide_all-records_codes.csv", sep=""))
  data2008 = spark_read_csv(sc, name = "data_tbl2008", path = paste(bucket_name,"data/micro/hmda_2008_nationwide_all-records_codes.csv", sep=""))
  data2009 = spark_read_csv(sc, name = "data_tbl2009", path = paste(bucket_name,"data/micro/hmda_2009_nationwide_all-records_codes.csv", sep=""))
  data2010 = spark_read_csv(sc, name = "data_tbl2010", path = paste(bucket_name,"data/micro/hmda_2010_nationwide_all-records_codes.csv", sep=""))
  data2011 = spark_read_csv(sc, name = "data_tbl2011", path = paste(bucket_name,"data/micro/hmda_2011_nationwide_all-records_codes.csv", sep=""))
  data2012 = spark_read_csv(sc, name = "data_tbl2012", path = paste(bucket_name,"data/micro/hmda_2012_nationwide_all-records_codes.csv", sep=""))
  data2013 = spark_read_csv(sc, name = "data_tbl2013", path = paste(bucket_name,"data/micro/hmda_2013_nationwide_all-records_codes.csv", sep=""))
  data2014 = spark_read_csv(sc, name = "data_tbl2014", path = paste(bucket_name,"data/micro/hmda_2014_nationwide_all-records_codes.csv", sep=""))
  data2015 = spark_read_csv(sc, name = "data_tbl2015", path = paste(bucket_name,"data/micro/hmda_2015_nationwide_all-records_codes.csv", sep=""))
  data2016 = spark_read_csv(sc, name = "data_tbl2016", path = paste(bucket_name,"data/micro/hmda_2016_nationwide_all-records_codes.csv", sep=""))
  data2017 = spark_read_csv(sc, name = "data_tbl2017", path = paste(bucket_name,"data/micro/hmda_2017_nationwide_all-records_codes.csv", sep=""))
  
  # let spark refresh the tables
  src_tbls(sc)
  
  # now we use sdf_bind_rows(), a sparklyr command to bind the rows of the dataset
  hmda_spark = sdf_bind_rows(data2007, data2008, data2009, data2010, data2011, data2012, data2013, data2014, data2015, data2016, data2017)
  
  # the register command allows to actually execute the command above. All sparklyr commands are 'idle' by default, i.e. they are only
  # executed whenever the intermediate output is needed
  sdf_register(hmda_spark, "data_tbl")
}



# Manipulate dataframe
hmda_spark = hmda_spark %>% 
  mutate(applicant_income_000s = as.numeric(as.character(applicant_income_000s)),
         price_to_inc = loan_amount_000s / applicant_income_000s,
         state_code = as.character(as.numeric(state_code)),
         action_taken  = as.numeric(action_taken),
         county_code = as.character(county_code),
         applicant_ethnicity=  as.character(applicant_ethnicity),
         applicant_sex = as.character(applicant_sex),
         applicant_race_1 = as.character(applicant_race_1),
         property_type = as.character(property_type),
         owner_occupancy = as.character(owner_occupancy),
         loan_purpose = as.character(loan_purpose),
         year = as.character(as_of_year),
         application_accepted = action_taken == 1) %>% 
  filter(!is.na(applicant_income_000s),
         !is.na(applicant_ethnicity),
         !is.na(applicant_sex),
         !is.na(loan_purpose),
         !is.na(year),
         applicant_sex %in% c("1","2"),
         applicant_race_1 %in% c("2","3","5"))

# add a proper state code column
hmda_spark = hmda_spark %>% 
  spark_apply(function(e) 
    data.frame(sprintf("%02d",as.numeric(e$state_code)), e), 
    names = c('state_code_fips', colnames(hmda_spark)))

sdf_register(hmda_spark, "hmda_spark")

# collect spark dataframe to memory to compare local spark vs local base R
if(!cloud) {
  hmda = hmda_spark %>% collect()
}


if(!cloud) {
  hmda= select(hmda, as_of_year, sequence_number, respondent_id, loan_type, price_to_inc, loan_amount_000s, applicant_income_000s, everything())
  hmda_group = hmda %>% group_by(as_of_year,county_fips) %>% summarise(avg_prc_to_inc = mean(price_to_inc, na.rm=TRUE))       
  hmda_group = hmda_group %>% mutate(log_price_to_inc = log(1+avg_prc_to_inc))
} else {
  hmda= select(hmda, as_of_year, sequence_number, respondent_id, loan_type, price_to_inc, loan_amount_000s, applicant_income_000s, everything())
  hmda_group = hmda %>% group_by(as_of_year,county_fips) %>% summarise(avg_prc_to_inc = mean(price_to_inc, na.rm=TRUE))  %>% collect()     
  hmda_group = hmda_group %>% mutate(log_price_to_inc = log(1+avg_prc_to_inc))
}


counties_fips = counties 
setwd(paste0(path, "/Paper/",sep=""))
for(year in c(2007:2016)) {
  pdf(paste0("USmap_",year,".pdf",sep=""),width = 10, height = 2.1*2.5)
  print(year)
  data_app = hmda_group %>% filter(as_of_year == year)
  household_data <- left_join(counties_fips,data_app,  by = "county_fips", all.x=TRUE) 
  
  myplot = household_data %>%
    ggplot(aes(long, lat, group = group, fill = log_price_to_inc)) +
    geom_polygon(color = NA) +
    coord_map(projection = "albers", lat0 = 39, lat1 = 45) +
    scale_fill_gradient(limits=c(0,2), aesthetics = "fill") +
    labs(fill = "Loan to Income") +
    annotate("text", x=-100, y=52, label= year)+
    theme(plot.margin=grid::unit(c(0,0,0,0), "mm"))
  print(myplot)
  dev.off()
}





