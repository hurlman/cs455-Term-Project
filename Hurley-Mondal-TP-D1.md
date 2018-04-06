# Post Recession Housing Price vs Population Analysis  
Mike Hurley  
Chiranjeb Mondal  
April 6, 2018  
CS455  

## Datasets  
[US Census Data 2009-2013](https://www2.census.gov/acs2013_5yr/summaryfile/2009-2013_ACSSF_All_In_2_Giant_Files(Experienced-Users-Only)/)  
[Zillow Economics Data](https://www.kaggle.com/zillow/zecon/data)  

### Supplementary Datasets  
[Cost of Living Index](https://www.numbeo.com/cost-of-living/rankings.jsp)  
[Employment and Wage Data](https://www.bls.gov/oes/current/oessrcma.htm)  

## Problem Characterization  
As the US recovered from the Great Recession after its peak in 2008, some metro areas took longer than others to recover economically.  Migration across the country was lower than its historical average during this time period.  There is a relationship between population change in a city and the housing prices of that city.  We wish to investigate this further, and include other factors that may drive up or down a population change, and how that affects housing prices.  What factors lead to an increase or decrease in home price?  Do home prices and population have any correlation at all?  How big of a factor is employment and cost of living? Does the type of job impact housing price?  We believe the post recession period to be a very unique time period of recent history, with people making conservative decisions with respect to moving, changing careers, and buying a home.  Because of this, the data would be less noisy than usual, and perhaps some useful insights can be gleaned. It is important to note that our analysis is not based on the charateristics of the house but it primarily focuses on the above social indicators.  

## Currently Published Work  
[Evaluating the Housing Market Since the Great Recession](https://www.corelogic.com/downloadable-docs/corelogic-peak-totrough-final-030118.pdf)  
[How the Great Recession Changed US Migration Patterns](http://w3001.apl.wisc.edu/pdfs/b01_16.pdf)  
[The Impact of Employment on Housing Prices](https://www.tcd.ie/Economics/TEP/2017/tep0417.pdf)  

## Analytic Plan  
We plan on looking at a set of around ten US metropolitan areas.  We want a mix of cities that range from very hard hit by the housing crash (Las Vegas, NV), to cities that were barely affected at all (Houston, TX).  Additionaly we want a mix of cities that had the biggest population and employment changes both up (New Orleans, LA) and down (Flint, MI). We will be comparing the trends of all of the data we can have and see if any patterns emerge.  The logical hypothesis is that job openings are followed by population growth, which is followed by housing price increase, which is followed by cost of living increase.  But is this actually true?  Based on our analysis of the data, we will come up with a model that can predict whether or not home prices or population in a city will increase or decrease.  

## Measuring Results  
Since the goal of the project will be to develop a model, we can measure the accuracy of our model by applying it to the cities in our dataset post-recession.  Additionally we can apply our model to other cities outside our original dataset, and see if their behavior over time fits in with our model.   
