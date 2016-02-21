//+------------------------------------------------------------------+
//|                                              QuandlReader0.5.mq4 |
//|                                  Copyright 2015, Quantrade Corp. |
//|                                         https://www.talaikis.com |
//+------------------------------------------------------------------+
#property copyright "Copyright 2015, Quantrade Corp."
#property link      "https: //www.talaikis.com"
#property version   "0.5"
#property strict

//#property indicator_buffers 1
//#property indicator_color1 clrNONE

// QUandl symbol
extern string Symbol    = "YAHOO/INDEX_VDAX";

//table name
extern string _tablename = "YAHOO_INDEX_VDAX";

//here's your Quandl ayth cde
extern string _authCode = "-----------------";

extern string host      = "localhost";
extern int    port      = 3306;
extern string user      = "root";
extern string password  = "Hg#1F8h^=GP5@4v0u9";
extern string dbName    = "lean";

datetime      endTime  = Time[Bars - 1];
datetime      thisTime = Time[0];

string        tmpEndTime  = TimeToString(endTime, TIME_DATE);
string        tmpThisTime = TimeToString(thisTime, TIME_DATE);

string        yEnd = StringSubstr(tmpEndTime, 0, 4);
string        mEnd = StringSubstr(tmpEndTime, 5, 2);
string        dEnd = StringSubstr(tmpEndTime, 8, 2);

string        yThis = StringSubstr(tmpThisTime, 0, 4);
string        mThis = StringSubstr(tmpThisTime, 5, 2);
string        dThis = StringSubstr(tmpThisTime, 8, 2);

string        endDate   = yThis + "-" + mThis + "-" + dThis;
string        startDate = yEnd + "-" + mEnd + "-" + dEnd;

#include <MQLMySQL.mqh>

double _quandlClose[];

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
//--- create timer
    EventSetTimer(Period() * 60);

//---
    return(INIT_SUCCEEDED);
}
//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
//--- destroy timer
    EventKillTimer();
}
//+------------------------------------------------------------------+
//| Expert tick function                                             |
//+------------------------------------------------------------------+
void OnTick()
{
//---

    ArrayResize(_quandlClose, Bars);
    int bars = Bars;
    int i;

    if (Refresh(1440) == TRUE)
    {
        string cookie = NULL, headers;
        char post[], result[];
        int    res;
        string Query;
        int    DB;

        //connect to database
        DB = MySqlConnect(host, user, password, dbName, port, 0, CLIENT_MULTI_STATEMENTS);

        if (DB == -1)
        {
            Print("Connection to <a href="http://www.talaikis.com/mysql/">MySQL</a> database failed! Error: " + MySqlErrorDescription);
        }
        else
        {
            Print("Connected to <a href="http://www.talaikis.com/mysql/">MySQL</a>! DB_ID#", DB);
        }

        //Print(SymbolRep+" rep symbol");

        // url, this url into allowed urls in options
        string _url = "https://www.quandl.com/api/v1/datasets/" + Symbol + ".csv?trim_start=" + startDate + "&trim_end=" + endDate + "&sort_order=asc&exclude_headers=true&auth_token="+_authCode;

        Print("requested url " + _url);

        //--- Reset the last error code
        ResetLastError();

        //--- Loading a html page from Google Finance
        int timeout = 5000; //--- Timeout below 1000 (1 sec.) is not enough for slow Internet connection

        res = WebRequest("GET", _url, cookie, NULL, timeout, post, 0, result, headers);

        //--- Checking errors
        if (res == -1)
        {
            Print("Error in WebRequest. Error code  =", GetLastError());
            //--- Perhaps the URL is not listed, display a message about the necessity to add the address
            MessageBox("Add the address '" + _url + "' in the list of allowed URLs on tab 'Expert Advisors'", "Error", MB_ICONINFORMATION);
        }
        else
        {
            //--- Load successfully
            PrintFormat("The file has been successfully loaded, File size =%d bytes.", ArraySize(result));

            //--- Save the data to a file
            int filehandle = FileOpen("Quandl_" + _tablename + ".csv", FILE_WRITE | FILE_BIN);

            //--- Checking errors
            if (filehandle != INVALID_HANDLE)
            {
                //--- Save the contents of the result[] array to a file
                FileWriteArray(filehandle, result, 0, ArraySize(result));

                //--- Close the file
                FileClose(filehandle);
            }
            else
            {
                Print("Error in FileOpen. Error code=", GetLastError());
            }
        }

        //table according to Qundl file structure
        Query = "CREATE TABLE IF NOT EXISTS `" + _tablename + "` (" +
                "DATE_TIME timestamp NOT NULL, " + // default CURRENT_TIMESTAMP
                "OPEN double(6,2) NOT NULL, " +
                "HIGH double(6,2) NOT NULL, " +
                "LOW double(6,2) NOT NULL, " +
                "CLOSE double(6,2) NOT NULL, " +
                "VOLUME int NOT NULL, " +
                "ADJCLOSE double(6,2) NOT NULL, " +
                "PRIMARY KEY  (DATE_TIME)" +
                ") ENGINE=MyISAM  DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci AUTO_INCREMENT=0";
        if (MySqlExecute(DB, Query))
        {
            Print("Table " + _tablename + " is created.");
        }
        else
        {
            Print("Table " + _tablename + " cannot be created. Error: ", MySqlErrorDescription);
        }


        for (int d = 0; d <= (Bars - 1); d++)
        {
            int i, k, handle;

            handle = FileOpen("Quandl_" + _tablename + ".csv", FILE_READ); //try to open file

            //check if exists
            if (handle == -1)
            {
                //return(0);
                Print("File does not exist at second point.");
            }

            //check if not empty
            if (FileSize(handle) == 0)
            {
                FileClose(handle);
                FileDelete("Quandl_" + _tablename + ".csv");
                FileClose(handle);
                Print("File is empty at second point. File deleted.");
            }

            //--- Split the string to substrings
            string yD, mD, dD;
            string qTime, tmpTime;
            tmpTime = TimeToString(Time[d], TIME_DATE);

            //while file exists
            while (!FileIsEnding(handle))
            {
                string to_split = FileReadString(handle);

                string lines[];     // An array to get strings
                string columns[][9];

                string sep        = "\n";     // A separator as a character
                string column_sep = ",";

                ushort u_sep;     // The code of the separator character
                ushort u_col_sep;

                //--- Get the separator code
                u_sep     = StringGetCharacter(sep, 0);
                u_col_sep = StringGetCharacter(column_sep, 0);

                if (to_split != "")     //if string not empty
                {
                    //--- Split the string to substrings
                    k = StringSplit(to_split, u_sep, lines);
                }

                //--- Now output all obtained strings
                if (k > 0)
                {
                    for (i = 0; i < k; i++)
                    {
                        //PrintFormat("lines[%d]=%s", i, lines[i]);
                        string temp = lines[i];
                        int    s    = StringSplit(temp, u_col_sep, columns);

                        //check if time column isn't empty
                        if (columns[i][0] != NULL)
                        {
                            yD = StringSubstr(columns[i][0], 0, 4);
                            mD = StringSubstr(columns[i][0], 5, 2);
                            dD = StringSubstr(columns[i][0], 8, 2);
                        }

                        qTime = yD + "." + mD + "." + dD;

                        //insert if time is found on file and get data into quandl buffer
                        if (qTime == tmpTime)
                        {
                            //Print("Time found in file. " + d);
                            //Print("qtime: " + qTime + " barTime " + tmpTime);
                            ArrayResize(_quandlClose, Bars);
                            
                            //change 6 to appropriate fields number
                            _quandlClose[d] = StrToDouble(columns[i][6]);

                            //data according to table structure
                            Query = "INSERT INTO `" + _tablename + "` (date_time, open, high, low, close, volume, adjclose) VALUES (\'" +
                                    columns[i][0] + "\'," +
                                    columns[i][1] + "," +
                                    columns[i][2] + "," +
                                    columns[i][3] + "," +
                                    columns[i][4] + "," +
                                    columns[i][5] + "," +
                                    columns[i][6] +
                                    ")";

                            if (MySqlExecute(DB, Query))
                            {
                                Print("Succeeded: ", Query);
                            }
                            else
                            {
                                Print("Error: ", MySqlErrorDescription);
                                Print("Error with: ", Query);
                            }
                        }
                    }          // close of iteration through file
                }              //end for each line in file
            }                  //end of while

            FileClose(handle); //close file
        }

    } //end of refresh
}     //close of OnTick

//+------------------------------------------------------------------+
//| Timer function                                                   |
//+------------------------------------------------------------------+
void OnTimer()
{
//---
}

//+------------------------------------------------------------------+

//update base only once a bar
bool Refresh(int _per)
{
    static datetime PrevBar;
    //Print("Refresh times. PrevBar: "+PrevBar);

    if (PrevBar != iTime(NULL, _per, 0))
    {
        PrevBar = iTime(NULL, _per, 0);
        return(true);
    }
    else
    {
        return(false);
    }
}