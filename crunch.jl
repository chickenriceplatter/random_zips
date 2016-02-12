using DataFrames

column_types = fill(UTF8String, (32,))

df = readtable("summary.csv", eltypes=column_types)

new_df = DataFrame()

new_df[:id] = df[:DESYNPUF_ID]
new_df[:state_code] = df[:SP_STATE_CODE]
new_df[:county_code] = df[:BENE_COUNTY_CD]

temp = []

for i in 1:size(df)[1]
  string = new_df[:state_code][i] * new_df[:county_code][i]
  push!(temp, string)
end

new_df[:ssa_code] = temp

column_types = fill(UTF8String, (4,))

ssa_fips = readtable("ssa_fips.csv",eltypes=column_types)

temp = []

for i in 1:size(new_df)[1]
  index = findfirst(ssa_fips[:ssa],new_df[:ssa_code][i])
  if index == 0
    push!(temp, "")
  else
    push!(temp, ssa_fips[:fips][index])
  end
end

new_df[:fips] = temp

column_types = fill(UTF8String, (2,))

fips_zip = readtable("fips_zip.csv",eltypes=column_types)

hash = Dict()

for i in 1:size(fips_zip)[1]
  key = fips_zip[:fips][i]

  if haskey(hash, key)
    push!(hash[key], fips_zip[:zip][i])
  else
    hash[key] = [fips_zip[:zip][i]]
  end
end

temp = []

for i in 1:size(new_df)[1]
  key = new_df[:fips][i]

  if haskey(hash, key)
    array = hash[key]

    len = length(array)

    zipcode = array[rand(1:len)]

    push!(temp, zipcode)
  else
    push!(temp, "")
  end
end

new_df[:zip] = temp

writetable("output.csv", new_df)
