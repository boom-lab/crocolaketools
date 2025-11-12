"""
Optimized version of get_subset_region that processes row groups incrementally
to avoid loading all data into memory at once.

Usage:
    include("argo_parquet_optimized.jl")
    df1 = get_subset_region_optimized(da.Dataset, variables=variables, lons=lons, lats=lats, dates=dates)
"""

using DataFrames, Dates
import Parquet2

function get_subset_region(ds2::Parquet2.Dataset; 
    lons=-180 .. 180, 
    lats=-90 .. 90, 
    dates=DateTime("1990-01-01T00:00:00") .. DateTime("2030-12-31T23:59:59"),
    variables=(:JULD, :LATITUDE, :LONGITUDE, :PRES, :TEMP, :PLATFORM_NUMBER),
    verbose=true)

    # Define filtering rules
    rule_juld = x -> in.(coalesce.(x, DateTime("2100-01-01T00:00:00")), Ref(dates))
    rule_latitude = x -> in.(coalesce.(x, NaN), Ref(lats))
    rule_longitude = x -> in.(coalesce.(x, NaN), Ref(lons))
    
    # Check which row groups contain relevant data
    rule_row_group = x ->   any(rule_latitude(Parquet2.load(x["LATITUDE"]))) && 
                            any(rule_longitude(Parquet2.load(x["LONGITUDE"]))) && 
                            any(rule_juld(Parquet2.load(x["JULD"])))

    row_groups = filter(rule_row_group, ds2.row_groups)
    
    if verbose
        println("Found $(length(row_groups)) matching row groups out of $(length(ds2.row_groups))")
    end
    
    if isempty(row_groups)
        if verbose
            println("No data found in specified region/time range")
        end
        return DataFrame([col => [] for col in variables]...)
    end
    
    # Process row groups one at a time and filter immediately
    filtered_dfs = DataFrame[]
    
    for (i, rg) in enumerate(row_groups)
        if verbose && i % 10 == 0
            println("Processing row group $i/$(length(row_groups))...")
        end
        
        # Load only this row group
        df_temp = DataFrame(rg, copycols=false)
        df_temp = select(df_temp, variables...)
        
        # Filter immediately before accumulating
        subset!(df_temp, :LATITUDE => rule_latitude, skipmissing=false)
        subset!(df_temp, :LONGITUDE => rule_longitude, skipmissing=false)
        subset!(df_temp, :JULD => rule_juld, skipmissing=false)
        
        # Only keep if there's data left after filtering
        if nrow(df_temp) > 0
            push!(filtered_dfs, df_temp)
        end
    end
    
    if isempty(filtered_dfs)
        if verbose
            println("No data matched filters after detailed filtering")
        end
        return DataFrame([col => [] for col in variables]...)
    end
    
    # Combine only the filtered results
    if verbose
        println("Combining $(length(filtered_dfs)) filtered row groups...")
    end
    
    df_result = vcat(filtered_dfs..., cols=:union)
    
    if verbose
        println("Final result: $(nrow(df_result)) rows")
    end
    
    return df_result
end
