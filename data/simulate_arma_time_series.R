# creates a dataframe with 1,000 time series simulated from an ARMA(2,2) process

data = data.frame(matrix(ncol = 3, nrow = 1000000)) 
colnames(data)=c("DATA", "ID", "ORDER")

for (i in 1:1000){

  print(paste0("Simulating series: ", i))
  
  index = ((1000*i)-999):(1000*i)
  
  ts.sim <- arima.sim(n = 1000, list(ar = c(0.89, -0.49), ma = c(-0.23, 0.25)),
            sd = sqrt(0.18))

  data$DATA[index] = ts.sim
  data$ID[index] = i
  data$ORDER[index] = 1:1000
  
}
