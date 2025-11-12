%% ===================================================================== %%
% Temperature map - CrocoLake PHY
% ======================================================================= %
%
% This example shows how to read and manipulate CrocoLake data.
% We will filter the data by pressure, time, and location. For each dataset 
% (Argo, GLODAP, SPray Gliders), we will then plot each location containing 
% a temperature measurement.
%
%% Setup
% Set up the reader. Here we generate a ParquetDatastore object of the 
% database (no need to know the details of what a ParquetDatastore object 
% exactly is).
% To read only some variables, create a `selectVariables` array with the 
% Argo parameters to read. Note that the dataset must load all the 
% variables to which filters are later applied.
% To read all the variables, just do not specify "SelectedVariableNames"
% when calling parquetDatastore().
%

parquetPath = fullfile("../crocolaketools/demo/parquet/demo_CROCOLAKE_PHY/");
location = matlab.io.datastore.FileSet(parquetPath,"FileExtensions",".parquet"); % for faster parsing
selectVariables = [...
    "DB_NAME",...
    "LATITUDE",...
    "LONGITUDE",...
    "JULD",...
    "PRES",...
    "TEMP",...
    "ABS_SAL_COMPUTED",...
    "CONSERVATIVE_TEMP_COMPUTED",...
    "SIGMA1_COMPUTED"
    ];
pds = parquetDatastore(...
            location, ...
            "FileExtensions",".parquet", ...
            "IncludeSubfolders", false, ...
            "OutputType", "table", ...    
            "VariableNamingRule","preserve", ...
            "SelectedVariableNames", selectVariables ...
        );

%% Filtering
% We now create the filters for our data. 
% We need first to create a RowFilter object from the dataset, then we
% populate it with the filters, and finally we will assign it back to the
% dataset.

% Generating RowFilter object
rf = rowfilter(pds);

% Creating filters on pressure, data quality and time. 'and' and 'or'
% operators are the bitwise operators '&' and '|' as usual in MATLAB.
% The supported relational operators are: <, <=, >, >=, ==, and ~= .
filter_pres = rf.("PRES") <= 20;
filter_lat = rf.("LATITUDE") <= 60 & rf.("LATITUDE") >= 0 ;
filter_lon = rf.("LONGITUDE") <= 0 & rf.("LONGITUDE") >= -90 ;

startTime = datetime(2010,1,1,0,0,0); % year, month, day, hour (24h format), min, sec
endTime   = datetime(2022,1,1,0,0,0); % year, month, day, hour (24h format), min, sec
filter_time = rf.("JULD") >= startTime & rf.("JULD") <= endTime ;

% Combining the filters in one and assigning it to the ParquetDataset
% object
filter = filter_pres & filter_time & filter_lat & filter_lon;
pds.RowFilter = filter;

%% Reading the data into memory, in parallel (timing the operation)
% uncomment the following lines to read data in parallel (slightly faster)
p = gcp("nocreate");
if isempty(p)
    tic
    parpool; % you can specificy the number of workers with 
             % parpool(nbWorkers); the default shoud be fine
    elapsed = toc;
    disp("Elapsed time to  create parallel environment: " + num2str(elapsed) + " seconds.")
end
tic;
dataPHY = readall(pds,UseParallel=true);
elapsed = toc;
disp("Elapsed time to read data into memory in parallel: " + num2str(elapsed) + " seconds.")

%% Reading the data into memory, serially (timing the operation)
% uncomment the following lines to read data serially (slower)
% tic;
% dataPHY = readall(pds,UseParallel=false);
% elapsed = toc;
% disp("Elapsed time to read data into memory serially: " + num2str(elapsed) + " seconds.")

%% Plotting target data
% Now we can make a scatter plot of the temperature data recorded
varName = 'TEMP';

% check that (lat0,lon0) are unique, otherwise average data
[G, LAT, LON] = findgroups(dataPHY.LATITUDE,dataPHY.LONGITUDE);
if height(dataPHY) ~= height(G)
    meanVar = splitapply(@mean, dataPHY.(varName), G);
    refTable = table( ...
        LAT, LON, meanVar, ...
        'VariableNames', {'LATITUDE', 'LONGITUDE', varName} ...
        );
else
    refTable = dataPHY;
end
percentiles = prctile(refTable.(varName), [10, 90]);

% Figure
f = figure("Position", [100 300 900 800]) ;
gx = geoaxes( ...
    'Basemap','None', ...
    'Grid','on' ...
    );
geobasemap('satellite');
handles = cell(1, 3);
count = 0;
db_names = ["ARGO","GLODAP","SprayGliders"];
for db_name = db_names
    count = count + 1;
    if db_name=="ARGO"
        colour = [0.8500 0.3250 0.0980];
    elseif db_name=="GLODAP"
        colour = [0.4660 0.6740 0.1880];
    else
        colour = [0.9290 0.6940 0.1250];
    end

    plotTable = refTable(strcmp(refTable.DB_NAME, db_name), :);
    handles{count} = geoscatter(...
            plotTable.LATITUDE, ...
            plotTable.LONGITUDE, ...
            10, ...
            'filled',...
            'Marker','o',...
            'MarkerFaceColor', colour...
            );

    colormap(gx,"copper");
    clim(gx, percentiles);
    hold on;

    % statistics
    dataStats = summary(plotTable);
    disp("Minimum temperature in " + db_name + " dataset: " + num2str(dataStats.TEMP.Min) + " Celsius");
    disp("Maximum temperature in " + db_name + " dataset: " + num2str(dataStats.TEMP.Max) + " Celsius");
    disp("Median temperature in " + db_name + " dataset: " + num2str(dataStats.TEMP.Median) + " Celsius");
end
% geolimits( [0,60], [-90,0] );
legend([handles{:}], db_names);
title("Temperature measurements in [0,60], [-90,0]");

%% Basic statistics
% We can also quickly investigate some statistics of the loaded data
dataStats = summary(dataPHY);
% and for example see the min, max, and median values of the temperatue
disp("Minimum temperature = " + num2str(dataStats.TEMP.Min) + " Celsius");
disp("Maximum temperature = " + num2str(dataStats.TEMP.Max) + " Celsius");
disp("Median temperature = " + num2str(dataStats.TEMP.Median) + " Celsius");
