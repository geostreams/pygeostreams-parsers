import requests
import json

# Define the JSON data

data ={
    "agent": {
        "@type": "cat:user",
        "user_id": "https://smartfarm-clowder.ncsa.illinois.edu/api/users/63f7cd0cb9b578215f3ad007"
    },
    "content": {
        "Creator": "Xiangmin (Sam) Sun,",
        "Creator's Email": "sunxm03@illinois.edu",
        "Other Researchers with Dataset Knowledge (with email)": "Carl J Bernacchi (bernacch@illinois.edu)",
        "Dataset Time Span": "November, 2020 — February, 2021",
        "File Format": "Comma-separated values(.csv)",
        "Variables": [
            "date	[yyyy-mm-dd]",
            "time	[HH:MM]",
            "DOY	[ddd.ddd]",
            "ALB_1_1_1	[%]	Albedo",
            "CHK__1_1_1	[OTHER]	A check sum feature to check integrity of data",
            "DAQM_T_1_1_1	[C]	Data Acquisition Module Temp",
            "VIN_1_1_1	[V]	Data Acquisition Module Voltage (Input Voltage)",
            "LWIN_1_1_1	[W/m^2]	Longwave incoming radiation",
            "LWOUT_1_1_1	[W/m^2]	Longwave outgoing radiation",
            "PPFD_1_1_1	[µmol/m^2/s]	Photosynthetic photon flux density (PAR)",
            "P_RAIN_1_1_1	[m]	Precipitation falling as rain (may under-estimate precipitation falling as snow)",
            "RH_1_1_1	[%]	Relative humidity",
            "RN_1_1_1	[W/m^2]	Net Radiation",
            "SHF_1_1_1	[W/m^2]	Soil Heat Flux at 5 cm depth",
            "SHF_2_1_1	[W/m^2]	Soil Heat Flux at 5 cm depth",
            "SHF_3_1_1	[W/m^2]	Soil Heat Flux at 5 cm depth",
            "SWC_1_1_1	[m^3/m^3]	Soil water content at 5 cm depth",
            "SWC_2_1_1	[m^3/m^3]	Soil water content at 5 cm depth",
            "SWC_3_1_1	[m^3/m^3]	Soil water content at 5 cm depth",
            "SWIN_1_1_1	[W/m^2]	Shortwave incoming radiation",
            "SWOUT_1_1_1	[W/m^2]	Shortwave outgoing radiation",
            "TA_1_1_1	[K]	Air Temperature; Mean temperature of ambient air",
            "TCNR4_C_1_1_1	[C]	temperature measurement from net radiometer independently of the model",
            "TS_1_1_1	[K]	Soil temperature at 5 cm depth",
            "TS_2_1_1	[K]	Soil temperature at 5 cm depth",
            "TS_3_1_1	[K]	Soil temperature at 5 cm depth",
            "daytime	[1=daytime]	Binary flag where 0 = nighttime, 1 = daytime, based on threshold of incident solar radiation (PPFD ≤ 5 μmol m−2 s−1)",
            "file_records	[#]	Number of valid records found in the raw file (or set of raw files)",
            "used_records	[#]	Number of valid records used for current the averaging period",
            "Tau	[kg+1m-1s-2]	Corrected momentum flux",
            "qc_Tau	[#]	Quality flag for momentum flux",
            "rand_err_Tau	[kg+1m-1s-2]	Random error for momentum flux",
            "H	[W+1m-2]	Corrected sensible heat flux",
            "qc_H	[#]	Quality flag for sensible heat flux",
            "rand_err_H	[W+1m-2]	Random error for momentum flux",
            "LE	[W+1m-2]	Corrected latent heat flux",
            "qc_LE	[#]	Quality flag latent heat flux",
            "rand_err_LE	[W+1m-2]	Random error for latent heat flux",
            "co2_flux	[µmol+1s-1m-2]	Corrected gas (CO2) flux",
            "qc_co2_flux	[#]	Quality flag for (CO2) flux",
            "rand_err_co2_flux	[µmol+1s-1m-2]	Random error for (CO2) flux",
            "h2o_flux	[mmol+1s-1m-2]	Corrected gas (H2O) flux",
            "qc_h2o_flux	[#]	Quality flag for (H2O) flux",
            "rand_err_h2o_flux	[mmol+1s-1m-2]	Random error for (H2O) flux",
            "ch4_flux	[µmol+1s-1m-2]	Corrected gas (CH4) flux",
            "qc_ch4_flux	[#]	Quality flag for (CH4) flux",
            "rand_err_ch4_flux	[µmol+1s-1m-2]	Random error for (CH4) flux",
            "H_strg	[W+1m-2]	Estimate of storage sensible heat flux",
            "LE_strg	[W+1m-2]	Estimate of storage latent heat flux",
            "co2_strg	[µmol+1s-1m-2]	Estimate of storage gas (CO2) flux",
            "h2o_strg	[mmol+1s-1m-2]	Estimate of storage gas (H2O) flux",
            "co2_v.adv	[µmol+1s-1m-2]	Estimate of vertical advection gas (CO2) flux",
            "h2o_v.adv	[mmol+1s-1m-2]	Estimate of vertical advection gas (H2O) flux",
            "co2_molar_density	[mmol+1m-3]	Measured or estimated molar density of gas (CO2)",
            "co2_mole_fraction	[µmol+1mol_a-1]	Measured or estimated mole fraction of gas (CO2)",
            "co2_mixing_ratio	[µmol+1mol_d-1]	Measured or estimated mixing ratio of gas (CO2)",
            "co2_time_lag	[s]	Time lag used to synchronize gas (CO2) time series",
            "co2_def_timelag	[1=default]	Flag: whether the reported time lag is the default (T = 1) or calculated (F = 0)",
            "h2o_molar_density	[mmol+1m-3]	Measured or estimated molar density of gas (H2O)",
            "h2o_mole_fraction	[mmol+1mol_a-1]	Measured or estimated mole fraction of gas (H2O)",
            "h2o_mixing_ratio	[mmol+1mol_d-1]	Measured or estimated mixing ratio of gas (H2O)",
            "h2o_time_lag	[s]	Time lag used to synchronize gas (H2O) time series",
            "h2o_def_timelag	[1=default]	Flag: whether the reported time lag is the default (T = 1) or calculated (F = 0)",
            "sonic_temperature	[K]	Mean temperature of ambient air as measured by the anemometer",
            "air_temperature	[K]	Mean temperature of ambient air, as measured by platinum resistance thermometer",
            "air_pressure	[Pa]	Mean pressure of ambient air, as measured by the gas analyzer",
            "air_density	[kg+1m-3]	Density of ambient air",
            "air_heat_capacity	[J+1kg-1K-1]	Specific heat at constant pressure of ambient air",
            "air_molar_volume	[m+3mol-1]	Molar volume of ambient air",
            "ET	[mm+1hour-1]	Evapotranspiration flux",
            "water_vapor_density	[kg+1m-3]	Ambient mass density of water vapor",
            "e	[Pa]	Ambient water vapor partial pressure",
            "es	[Pa]	Ambient water vapor partial pressure at saturation",
            "specific_humidity	[kg+1kg-1]	Ambient specific humidity on a mass basis",
            "RH	[%]	Ambient relative humidity",
            "VPD	[Pa]	Ambient water vapor pressure deficit",
            "Tdew	[K]	Ambient dew point temperature",
            "u_unrot	[m+1s-1]	Wind component along the u anemometer axis",
            "v_unrot	[m+1s-1]	Wind component along the v anemometer axis",
            "w_unrot	[m+1s-1]	Wind component along the w anemometer axis",
            "u_rot	[m+1s-1]	Rotated u wind component (mean wind speed)",
            "v_rot	[m+1s-1]	Rotated v wind component (should be zero)",
            "w_rot	[m+1s-1]	Rotated w wind component (should be zero)",
            "wind_speed	[m+1s-1]	Mean wind speed",
            "max_wind_speed	[m+1s-1]	Maximum instantaneous wind speed",
            "wind_dir	[deg_from_north]	Direction from which the wind blows, with respect to Geographic or Magnetic north",
            "yaw	[deg]	First rotation angle",
            "pitch	[deg]	Second rotation angle",
            "roll	[deg]	roll angle measured about the new x-axis in rotation",
            "u* (or u.)	[m+1s-1]	Friction velocity",
            "TKE	[m+2s-2]	Turbulent kinetic energy",
            "L	[m]	Monin-Obukhov length",
            "X.z.d..L	[#]	Monin-Obukhov stability parameter",
            "bowen_ratio	[#]	Sensible heat flux to latent heat flux ratio",
            "T.	[K]	Scaling temperature",
            "model	[0=KJ/1=KM/2=HS]	Model for footprint estimation (Kljun et al. method63 generally used [model = 0], but not suitable for certain atmospheric conditions, when Kormann and Meixner method64 is used instead [model = 1])",
            "x_peak	[m]	Along-wind distance providing <1% contribution to turbulent fluxes",
            "x_offset	[m]	Along-wind distance providing the highest (peak) contribution to turbulent fluxes",
            "x_10.	[m]	Along-wind distance providing 10% (cumulative) contribution to turbulent fluxes",
            "x_30.	[m]	Along-wind distance providing 30% (cumulative) contribution to turbulent fluxes",
            "x_50.	[m]	Along-wind distance providing 50% (cumulative) contribution to turbulent fluxes",
            "x_70.	[m]	Along-wind distance providing 70% (cumulative) contribution to turbulent fluxes",
            "x_90.	[m]	Along-wind distance providing 90% (cumulative) contribution to turbulent fluxes",
            "un_Tau	[kg+1m-1s-2]	Uncorrected momentum flux",
            "Tau_scf	[#]	Spectral correction factor for momentum flux",
            "un_H	[W+1m-2]	Uncorrected sensible heat flux",
            "H_scf	[#]	Spectral correction factor for sensible heat flux",
            "un_LE	[W+1m-2]	Uncorrected latent heat flux",
            "LE_scf	[#]	Spectral correction factor for latent heat flux",
            "un_co2_flux	[µmol+1s-1m-2]	Uncorrected gas (CO2) flux",
            "co2_scf	[#]	Spectral correction factor for gas (CO2) flux",
            "un_h2o_flux	[mmol+1s-1m-2]	Uncorrected gas (H2O) flux",
            "h2o_scf	[#]	Spectral correction factor for gas (H2O) flux",
            "spikes_hf	8u/v/w/ts/co2/h2o/ch4/none	Hard flags for individual variables for spike test",
            "amplitude_resolution_hf	8u/v/w/ts/co2/h2o/ch4/none	Hard flags for individual variables for amplitude resolution",
            "drop_out_hf	8u/v/w/ts/co2/h2o/ch4/none	Hard flags for individual variables for drop-out test",
            "absolute_limits_hf	8u/v/w/ts/co2/h2o/ch4/none	Hard flags for individual variables for absolute limits",
            "skewness_kurtosis_hf	8u/v/w/ts/co2/h2o/ch4/none	Hard flags for individual variables for skewness and kurtosis",
            "skewness_kurtosis_sf	8u/v/w/ts/co2/h2o/ch4/none	Soft flags for individual variables for skewness and kurtosis test",
            "discontinuities_hf	8u/v/w/ts/co2/h2o/ch4/none	Hard flags for individual variables for discontinuities test",
            "discontinuities_sf	8u/v/w/ts/co2/h2o/ch4/none	Soft flags for individual variables for discontinuities test",
            "timelag_hf	8co2/h2o/ch4/none	Hard flags for gas concentration for time lag test",
            "timelag_sf	8co2/h2o/ch4/none	Soft flags for gas concentration for time lag test",
            "attack_angle_hf	8aa	Hard flag for attack angle test",
            "non_steady_wind_hf	8U	Hard flag for non-steady horizontal test",
            "u_spikes	[#]	Number of spikes detected and eliminated for variable var",
            "v_spikes	[#]	Number of spikes detected and eliminated for variable var",
            "w_spikes	[#]	Number of spikes detected and eliminated for variable var",
            "ts_spikes	[#]	Number of spikes detected and eliminated for variable var",
            "co2_spikes	[#]	Number of spikes detected and eliminated for variable var",
            "h2o_spikes	[#]	Number of spikes detected and eliminated for variable var",
            "u_var	[m+2s-2]	Variance of u",
            "v_var	[m+2s-2]	Variance of v",
            "w_var	[m+2s-2]	Variance of w",
            "ts_var	[K+2]	Variance of ts",
            "co2_var	--	Variance of co2",
            "h2o_var	--	Variance of h2o",
            "w.ts_cov	[m+1s-1K+1]	Covariance between w and ts",
            "w.co2_cov	--	Covariance between w and co2",
            "w.h2o_cov	--	Covariance between w and h2o",
         ],
    "Spatial Resolution":"data from point measurement.\
Location of the tower:\
Latitude: 40.0061788\
Longitude:-88.2903634",
    "Creation Information":[
        "Include information about how the dataset was created (such as what was being studied to produce the data), any specific instruments used, descriptions of scripts/code used to calculate/create the data files. Describe the process that would be needed to update or reproduce the dataset. If already documented elsewhere, can be a citation or a link to another document. At each date, data was collected at multiple locations in the field. N/A indicated no sample as grass pathway in the field.\
        Biomet measurements include meteorological as well as ecological (e.g., soil and vegetation) measurements.Biomet measurements are used to improved flux computation and other purposes. Biomet data are typically sampled at lower rates than the eddy covariance data (e.g., 10Hz in our site) and averaged over longer time periods (e.g., 30 minutes in our study ). Biomet data are distinct from high-speed eddy data.\
One issue with this flux data is that the sensible heat 'H' is consistently 'NA', which was caused by an erroneous setting in this express processing, and will be corrected in the advanced processing and future express processing data."
        ]
        }
}


# Convert the data to JSON format
json_data = json.dumps(data)

# Define the URL to send the POST request to
url = "https://smartfarm-clowder.ncsa.illinois.edu/api/datasets/6463b2e2e4b02d603bd1bfb5/metadata.jsonld"

# Set the content type header to application/json
headers = {'Content-type': 'application/json'}

# Define the authentication credentials
username = "mohanar2@illinois.edu"
password = "Arpa1234!"

# Send the POST request with the JSON data in the body and authentication credentials
response = requests.post(url, data=json_data, headers=headers, auth=(username, password))

# Check the response status code
if response.status_code == 200:
    print("POST request successful")
else:
    print("POST request failed")