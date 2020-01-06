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

## Compute Regressions
# ols regression on spark

# ols regression on local machine using Sparks's MLlib
start_time <- Sys.time()
lm_model_hmda = hmda_spark %>% 
  ml_linear_regression(application_accepted ~ applicant_income_000s + applicant_race_1 + applicant_sex+
                         loan_purpose+ year)
lm_model_hmda$summary$t_values()
lm_model_hmda$summary$p_values()
summary(lm_model_hmda)
end_time <- Sys.time()
end_time - start_time

# ols regression on local machine using base R
hmda = hmda_spark %>% collect()
start_time <- Sys.time()
summary(lm(application_accepted ~ applicant_income_000s + applicant_race_1 + applicant_sex+
             loan_purpose+ year, data= hmda))
end_time <- Sys.time()
end_time - start_time



# probit regression
# probit regression on local machine using Sparks's MLlib 
start_time <- Sys.time()
glm_model = hmda_spark %>% 
  ml_generalized_linear_regression(application_accepted ~ applicant_income_000s + applicant_race_1 + 
                                     applicant_sex+loan_purpose+ year,
                                   family = "binomial",link = "probit")

glm_model$coefficients
glm_model$summary$p_values()
end_time <- Sys.time()
end_time - start_time


# probit regression on local machine using base R's glm()
if(!cloud) {
start_time <- Sys.time()
summary(glm(application_accepted ~ applicant_income_000s + applicant_race_1 + applicant_sex+
              loan_purpose+ year,
            family=binomial(link='probit'),
            data = hmda))
end_time <- Sys.time()
end_time - start_time
}

# logit regression
# lobit regression on local machine using Sparks's MLlib
start_time <- Sys.time()
glm_model = hmda_spark %>% 
  ml_generalized_linear_regression(application_accepted ~ applicant_income_000s + applicant_race_1 + applicant_sex+
                                     loan_purpose+ year,
                                   family = "binomial",
                                   link = "logit")
glm_model$coefficients
glm_model$summary$p_values()
end_time <- Sys.time()
end_time - start_time

# logit regression on local machine using base R's glm()
if(!cloud) {
start_time <- Sys.time()
summary(glm(application_accepted ~ applicant_income_000s + applicant_race_1 + applicant_sex+
              loan_purpose+ year,
            family=binomial(link='logit'),
            data = hmda))
end_time <- Sys.time()
end_time - start_time
}
