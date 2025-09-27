//+------------------------------------------------------------------+
//|                                     ClickHouseTicksEA.mq5        |
//|                      Copyright 2023, MetaQuotes Software Corp.   |
//|                                             https://www.mql5.com |
//+------------------------------------------------------------------+
#property copyright "Copyright 2023, MetaQuotes Software Corp."
#property link      "https://www.mql5.com"
#property version   "1.00"

#include <Trade\SymbolInfo.mqh>
#include <Arrays\ArrayString.mqh>

//--- Input parameters
input string InpClickHouseHost = "127.0.0.1";      // ClickHouse Host
input int    InpClickHousePort = 80;               // ClickHouse Port
input string InpClickHouseUser = "default";        // ClickHouse Username
input string InpClickHousePassword = "changeme1";  // ClickHouse Password
input string InpClickHouseDatabase = "default";    // ClickHouse Database
input int    InpSendIntervalSeconds = 2;          // Send Interval (seconds)
input int    InpMaxTicksPerBatch = 10000;            // Max Ticks Per Batch
input bool   InpDebugMode = false;                 // Debug Mode
input int    InpHistoricalTicksDays = 0;           // Historical Ticks Days (0 = no historical data)
input bool   InpResumeFromLastTick = true;         // Resume from Last Tick in Database

//--- Global variables
CSymbolInfo m_symbol_info;                         // Symbol info object
CArrayString m_symbols;                            // Array of symbols to track
CArrayString m_symbol_latest_times;                // Array to store latest tick times for each symbol
string m_ticks_data[];                             // Array to store tick data
datetime m_last_send_time = 0;                     // Last send time
int m_http_timeout = 5000;                         // HTTP timeout in milliseconds
bool m_symbols_subscribed = false;                 // Flag to check if symbols are subscribed
bool m_resume_enabled = true;                      // Runtime flag for resume functionality (can be modified)

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
   //--- Initialize tick data array
   ArrayResize(m_ticks_data, InpMaxTicksPerBatch);
   for(int i = 0; i < InpMaxTicksPerBatch; i++)
   {
      m_ticks_data[i] = "";
   }

   //--- Get all available symbols
   if(!GetAllSymbols())
   {
      Print("Failed to get symbols. EA will be terminated.");
      return(INIT_FAILED);
   }

   //--- Subscribe to all symbols for tick data
   if(!SubscribeToAllSymbols())
   {
      Print("Failed to subscribe to symbols. EA will be terminated.");
      return(INIT_FAILED);
   }

   Print("ClickHouse Ticks EA initialized successfully.");
   Print("Tracking ", m_symbols.Total(), " symbols.");
   Print("ClickHouse connection: ", InpClickHouseHost, ":", InpClickHousePort);

   //--- Initialize the resume enabled flag from input parameter
   m_resume_enabled = InpResumeFromLastTick;

   //--- Get latest tick timestamps from ClickHouse if resume is enabled
   if(m_resume_enabled)
   {
      Print("Retrieving latest tick timestamps from ClickHouse...");
      if(!GetLatestTickTimestampsFromClickHouse())
      {
         Print("Failed to retrieve latest tick timestamps from ClickHouse. Will use current time.");
      }
   }

   //--- Get historical ticks if requested or if resume is enabled
   if(InpHistoricalTicksDays > 0 || m_resume_enabled)
   {
      if(m_resume_enabled)
         Print("Retrieving missing ticks since last recorded timestamp...");
      else
         Print("Retrieving historical ticks for the last ", InpHistoricalTicksDays, " days...");

      if(!GetHistoricalTicks())
      {
         Print("Failed to retrieve historical ticks. EA will continue with real-time ticks only.");
      }
   }

   //--- Set timer for sending data
   EventSetTimer(InpSendIntervalSeconds);

   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   //--- Kill timer
   EventKillTimer();

   //--- Send any remaining ticks before deinitialization
   if(ArraySize(m_ticks_data) > 0)
   {
      SendTicksToClickHouse();
   }

   //--- Note: We're not unsubscribing from symbols to keep them in Market Watch
   //--- This prevents symbols from disappearing when the EA is removed
   if(InpDebugMode)
      Print("Keeping symbols subscribed in Market Watch.");

   Print("ClickHouse Ticks EA deinitialized.");
}

//+------------------------------------------------------------------+
//| Expert timer function                                            |
//+------------------------------------------------------------------+
void OnTimer()
{
   //--- Send accumulated ticks to ClickHouse
   SendTicksToClickHouse();
}

//+------------------------------------------------------------------+
//| Tick function                                                    |
//+------------------------------------------------------------------+
void OnTick()
{
   //--- Process ticks from all subscribed symbols
   ProcessAllSymbolsTicks();
}

//+------------------------------------------------------------------+
//| Get all available symbols                                        |
//+------------------------------------------------------------------+
bool GetAllSymbols()
{
   m_symbols.Clear();

   //--- Get total symbols in market watch
   int total = SymbolsTotal(true);

   if(total <= 0)
   {
      Print("No symbols found in Market Watch.");
      return false;
   }

   //--- Add all symbols to our array
   for(int i = 0; i < total; i++)
   {
      string symbol = SymbolName(i, true);
      m_symbols.Add(symbol);

      if(InpDebugMode)
         Print("Added symbol: ", symbol);
   }

   return true;
}

//+------------------------------------------------------------------+
//| Subscribe to all symbols for tick data                           |
//+------------------------------------------------------------------+
bool SubscribeToAllSymbols()
{
   if(m_symbols_subscribed)
      return true;

   int total = m_symbols.Total();
   int success_count = 0;

   for(int i = 0; i < total; i++)
   {
      string symbol = m_symbols.At(i);
      if(SymbolSelect(symbol, true))
      {
         success_count++;

         if(InpDebugMode)
            Print("Subscribed to symbol: ", symbol);
      }
      else
      {
         Print("Failed to subscribe to symbol: ", symbol);
      }
   }

   if(success_count == total)
   {
      m_symbols_subscribed = true;
      Print("Successfully subscribed to all ", total, " symbols.");
      return true;
   }
   else
   {
      Print("Successfully subscribed to ", success_count, " out of ", total, " symbols.");
      return success_count > 0; // Return true if at least one symbol was subscribed
   }
}

//+------------------------------------------------------------------+
//| Unsubscribe from all symbols                                     |
//+------------------------------------------------------------------+
void UnsubscribeFromAllSymbols()
{
   if(!m_symbols_subscribed)
      return;

   int total = m_symbols.Total();

   for(int i = 0; i < total; i++)
   {
      string symbol = m_symbols.At(i);
      // Only unsubscribe if the symbol was explicitly subscribed by this EA
      // Check if the symbol is currently selected in Market Watch before unsubscribing
      if(SymbolInfoInteger(symbol, SYMBOL_VISIBLE) && SymbolSelect(symbol, false))
      {
         if(InpDebugMode)
            Print("Unsubscribed from symbol: ", symbol);
      }
      else if(InpDebugMode)
      {
         Print("Symbol ", symbol, " was not subscribed or already removed from Market Watch.");
      }
   }

   m_symbols_subscribed = false;
   Print("Finished unsubscribing from symbols tracked by this EA.");
}

//+------------------------------------------------------------------+
//| Get historical ticks for all symbols                             |
//+------------------------------------------------------------------+
bool GetHistoricalTicks()
{
   //--- Check if we need to get historical ticks
   if(InpHistoricalTicksDays <= 0 && !m_resume_enabled)
   {
      if(InpDebugMode)
         Print("Historical ticks retrieval disabled (InpHistoricalTicksDays = ", InpHistoricalTicksDays, ", m_resume_enabled = ", m_resume_enabled, ")");
      return true;
   }

   //--- Calculate end time (current time)
   datetime end_time = TimeCurrent();

   //--- Determine start time based on settings
   if(m_resume_enabled)
   {
      //--- For resume mode, we'll get the latest timestamp for each symbol individually
      if(InpDebugMode)
         Print("Resume mode enabled. Will retrieve ticks from last recorded timestamp for each symbol.");
   }
   else
   {
      //--- For historical mode, use fixed days
      datetime start_time = end_time - (InpHistoricalTicksDays * 24 * 60 * 60);

      if(InpDebugMode)
      {
         Print("Retrieving historical ticks from ", TimeToString(start_time), " to ", TimeToString(end_time));
         Print("Total symbols to process: ", m_symbols.Total());
      }
   }

   int total_symbols = m_symbols.Total();
   int total_ticks_retrieved = 0;

   //--- First pass: collect all ticks from all symbols without delays
   if(InpDebugMode)
      Print("Starting first pass: collecting ticks from all symbols...");

   for(int i = 0; i < total_symbols; i++)
   {
      string symbol = m_symbols.At(i);
      datetime start_time = 0;

      //--- Determine start time for this symbol
      if(m_resume_enabled)
      {
         //--- Get the latest timestamp for this symbol from ClickHouse
         datetime symbol_start_time = GetLatestTickTimestampForSymbol(symbol);

         if(symbol_start_time > 0)
         {
            //--- Add 1 second to the latest time to avoid duplicates
            start_time = symbol_start_time + 1;

            if(InpDebugMode)
               Print("Resuming from ", TimeToString(start_time), " for symbol: ", symbol);
         }
         else
         {
            //--- No previous data for this symbol, use current time
            start_time = end_time;

            if(InpDebugMode)
               Print("No previous data found for symbol: ", symbol, ". Starting from current time.");
         }
      }
      else
      {
         //--- For historical mode, use fixed days
         start_time = end_time - (InpHistoricalTicksDays * 24 * 60 * 60);
      }

      //--- Skip if start time is after or equal to end time
      if(start_time >= end_time)
      {
         if(InpDebugMode)
            Print("Skipping symbol ", symbol, " as start time (", TimeToString(start_time), ") is after or equal to end time (", TimeToString(end_time), ")");
         continue;
      }

      if(InpDebugMode)
         Print("Collecting ticks for symbol: ", symbol, " from ", TimeToString(start_time), " to ", TimeToString(end_time));

      //--- Get historical ticks for this symbol
      MqlTick ticks_array[];
      // Explicitly use the datetime range overload of CopyTicks
      int ticks_count = CopyTicks(symbol, ticks_array, COPY_TICKS_ALL, (datetime)start_time, (datetime)end_time);

      if(ticks_count <= 0)
      {
         if(InpDebugMode)
            Print("No ticks found for symbol: ", symbol, " in the specified time range. Error: ", GetLastError());
         continue;
      }

      if(InpDebugMode)
         Print("Retrieved ", ticks_count, " ticks for symbol: ", symbol);

      //--- Process each tick immediately without delays
      for(int j = 0; j < ticks_count; j++)
      {
         //--- Filter ticks by the requested time range (CopyTicks can sometimes return extra data)
         datetime tick_time = ticks_array[j].time;
         if(tick_time < start_time || tick_time > end_time)
            continue;

         //--- Format tick data as JSON
         string tick_json = FormatTickAsJson(symbol, ticks_array[j]);

         //--- Add to array
         AddTickToArray(tick_json);
         total_ticks_retrieved++;

         //--- Send batch if it's full
         if(total_ticks_retrieved % InpMaxTicksPerBatch == 0)
         {
            SendTicksToClickHouse();
         }
      }
   }

   //--- Send any remaining ticks after processing all symbols
   SendTicksToClickHouse();

   if(m_resume_enabled)
      Print("Resume ticks retrieval completed. Total ticks retrieved: ", total_ticks_retrieved);
   else
      Print("Historical ticks retrieval completed. Total ticks retrieved: ", total_ticks_retrieved);

   return true;
}

//+------------------------------------------------------------------+
//| Process ticks from all symbols                                   |
//+------------------------------------------------------------------+
void ProcessAllSymbolsTicks()
{
   int total = m_symbols.Total();

   for(int i = 0; i < total; i++)
   {
      string symbol = m_symbols.At(i);

      //--- Get tick data for this symbol
      MqlTick tick;
      if(SymbolInfoTick(symbol, tick))
      {
         //--- Format tick data as JSON
         string tick_json = FormatTickAsJson(symbol, tick);

         //--- Add to array
         AddTickToArray(tick_json);

         if(InpDebugMode)
            Print("Processed tick for symbol: ", symbol);
      }
   }
}

//+------------------------------------------------------------------+
//| Format tick data as JSON                                         |
//+------------------------------------------------------------------+
string FormatTickAsJson(string symbol, const MqlTick &tick)
{
   //--- Get UTC time from MQL5 time
   // Note: tick.time is already in UTC seconds since epoch
   datetime utc_time = tick.time;

   //--- Adjust time from UTC+5 to UTC+3 by subtracting 2 hours (7200 seconds)
   utc_time -= 7200;

   //--- Convert to TimeStruct to get individual components
   MqlDateTime time_struct;
   TimeToStruct(utc_time, time_struct);

   //--- Format year, month, day, hour, minute, second with leading zeros
   string year = IntegerToString(time_struct.year);
   string month = IntegerToString(time_struct.mon);
   if(StringLen(month) < 2)
      month = "0" + month;

   string day = IntegerToString(time_struct.day);
   if(StringLen(day) < 2)
      day = "0" + day;

   string hour = IntegerToString(time_struct.hour);
   if(StringLen(hour) < 2)
      hour = "0" + hour;

   string minute = IntegerToString(time_struct.min);
   if(StringLen(minute) < 2)
      minute = "0" + minute;

   string second = IntegerToString(time_struct.sec);
   if(StringLen(second) < 2)
      second = "0" + second;

   //--- Get milliseconds from time_msc
   ulong time_msc = tick.time_msc;
   int milliseconds = (int)(time_msc % 1000);
   string ms_str = IntegerToString(milliseconds);
   if(StringLen(ms_str) < 3)
      ms_str = StringSubstr("000", 0, 3 - StringLen(ms_str)) + ms_str;

   //--- Format as ISO 8601 with nanoseconds (DateTime64(9) format)
   string iso_time = year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second + "." + ms_str + "000000";

   //--- Format as JSON
   string json = "{";
   json += "\"symbol\":\"" + symbol + "\",";
   json += "\"time\":\"" + iso_time + "\",";
   json += "\"bid\":" + DoubleToString(tick.bid, 5) + ",";
   json += "\"ask\":" + DoubleToString(tick.ask, 5);
   json += "}";

   return json;
}

//+------------------------------------------------------------------+
//| Add tick to array                                                |
//+------------------------------------------------------------------+
void AddTickToArray(string tick_json)
{
   //--- Find first empty slot
   int size = ArraySize(m_ticks_data);
   for(int i = 0; i < size; i++)
   {
      if(m_ticks_data[i] == "")
      {
         m_ticks_data[i] = tick_json;

         if(InpDebugMode)
            Print("Added tick to array at position ", i);

         //--- If we've reached the batch size, send immediately
         if(i == size - 1)
         {
            SendTicksToClickHouse();
         }

         return;
      }
   }
}

//+------------------------------------------------------------------+
//| Send ticks to ClickHouse                                         |
//+------------------------------------------------------------------+
void SendTicksToClickHouse()
{
   //--- Check if we have any ticks to send
   int size = ArraySize(m_ticks_data);
   if(size <= 0 || m_ticks_data[0] == "")
   {
      if(InpDebugMode)
         Print("No ticks to send.");
      return;
   }

   //--- Prepare data for sending
   string data = "";
   int count = 0;

   for(int i = 0; i < size; i++)
   {
      if(m_ticks_data[i] != "")
      {
         if(count > 0)
            data += ",";

         data += m_ticks_data[i];
         count++;
      }
      else
         break;
   }

   if(count <= 0)
   {
      if(InpDebugMode)
         Print("No valid ticks to send.");
      return;
   }

   //--- Create HTTP request
   string headers = "Content-Type: application/json\r\n";
   string url = "http://" + InpClickHouseHost + ":" + IntegerToString(InpClickHousePort) + "/";
   string query = "?user=" + InpClickHouseUser + "&password=" + InpClickHousePassword;

   //--- Build INSERT query
   string insert_query = "INSERT INTO " + InpClickHouseDatabase + ".ticks FORMAT JSONEachRow";
   string result;
   char post_data[];
   char response[];

   //--- Convert string to char array
   StringToCharArray(data, post_data, 0, StringLen(data));

   //--- Send HTTP request
   int timeout = m_http_timeout;
   int res = WebRequest("POST", url + query + "&query=" + insert_query, headers, timeout, post_data, response, headers);

   //--- Check result
   if(res == 200)
   {
      //--- Clear sent ticks
      for(int i = 0; i < count; i++)
      {
         m_ticks_data[i] = "";
      }

      //--- Update last send time
      m_last_send_time = TimeCurrent();

      if(InpDebugMode)
      {
         Print("Sent ", count, " ticks to ClickHouse.");
         result = CharArrayToString(response);
         Print("Server response: ", result);
      }
   }
   else
   {
      string error_msg = "Failed to send ticks to ClickHouse. Error code: " + IntegerToString(res);

      //--- Try to get error details
      if(res > 0)
      {
         result = CharArrayToString(response);
         error_msg += ". Response: " + result;
      }

      Print(error_msg);

      //--- If we have a connection error, keep the ticks for next attempt
      if(res < 0)
      {
         Print("Will retry sending ticks on next interval.");
      }
      else
      {
         //--- Clear ticks even on error to prevent memory buildup
         for(int i = 0; i < count; i++)
         {
            m_ticks_data[i] = "";
         }
      }
   }
}

//+------------------------------------------------------------------+
//| Get latest tick timestamps from ClickHouse for all symbols       |
//+------------------------------------------------------------------+
bool GetLatestTickTimestampsFromClickHouse()
{
   //--- Check if resume functionality is enabled
   if(!m_resume_enabled)
   {
      if(InpDebugMode)
         Print("Resume from last tick is disabled. Will use current time.");
      return true;
   }

   //--- Clear the array
   m_symbol_latest_times.Clear();

   //--- Create HTTP request
   string headers = "Content-Type: application/json\r\n";
   string url = "http://" + InpClickHouseHost + ":" + IntegerToString(InpClickHousePort) + "/";
   string query = "?user=" + InpClickHouseUser + "&password=" + InpClickHousePassword;

   //--- Build SELECT query to get latest timestamps for all symbols
   string select_query = "SELECT symbol, toString(max(time)) as latest_time FROM " + InpClickHouseDatabase + ".ticks GROUP BY symbol FORMAT JSONEachRow";
   string result;
   char response[];

   //--- Send HTTP request
   int timeout = m_http_timeout;
   char data[];
   int res = WebRequest("GET", url + query + "&query=" + select_query, headers, timeout, data, response, headers);

   //--- Check result
   if(res == 200)
   {
      result = CharArrayToString(response);

      if(InpDebugMode)
         Print("ClickHouse response: ", result);

      //--- Check if the response is empty or contains only whitespace
      StringTrimLeft(result);
      StringTrimRight(result);
      if(result == "")
      {
         if(InpDebugMode)
            Print("Empty response from ClickHouse. This might be normal if no data exists yet.");
         return true; // This is not an error, just no data yet
      }

      //--- Parse the JSON response to extract symbol and latest_time
      //--- Split response by lines to get individual JSON objects
      string lines[];
      StringSplit(result, '\n', lines);

      int total_lines = ArraySize(lines);
      int parsed_count = 0;

      for(int i = 0; i < total_lines; i++)
      {
         if(lines[i] == "") continue;

         //--- Parse JSON to extract symbol and time
         string symbol = "";
         string time_str = "";

         //--- Extract symbol
         int symbol_pos = StringFind(lines[i], "\"symbol\":\"");
         if(symbol_pos >= 0)
         {
            symbol_pos += 10; // Move past "\"symbol\":\""
            int symbol_end = StringFind(lines[i], "\"", symbol_pos);
            if(symbol_end > symbol_pos)
               symbol = StringSubstr(lines[i], symbol_pos, symbol_end - symbol_pos);
         }

         //--- Extract time
         int time_pos = StringFind(lines[i], "\"latest_time\":\"");
         if(time_pos >= 0)
         {
            time_pos += 15; // Move past "\"latest_time\":\""
            int time_end = StringFind(lines[i], "\"", time_pos);
            if(time_end > time_pos)
               time_str = StringSubstr(lines[i], time_pos, time_end - time_pos);
         }

         if(symbol != "" && time_str != "")
         {
            //--- Validate the time string format (basic check)
            if(StringLen(time_str) >= 19) // Minimum length for "YYYY-MM-DD HH:MM:SS"
            {
               //--- Store the latest time for this symbol
               m_symbol_latest_times.Add(symbol + "|" + time_str);
               parsed_count++;

               if(InpDebugMode)
                  Print("Latest tick time for ", symbol, ": ", time_str);
            }
            else if(InpDebugMode)
            {
               Print("Invalid time format for symbol ", symbol, ": ", time_str);
            }
         }
         else if(InpDebugMode)
         {
            Print("Failed to parse JSON line: ", lines[i]);
         }
      }

      if(InpDebugMode)
         Print("Successfully parsed ", parsed_count, " latest timestamps out of ", total_lines, " lines.");

      //--- If we couldn't parse any data but got a response, it might be an error message
      if(parsed_count == 0 && total_lines > 0)
      {
         Print("Warning: Could not parse any valid timestamp data from ClickHouse response.");
         Print("Response was: ", result);
      }

      return true;
   }
   else
   {
      string error_msg = "Failed to get latest timestamps from ClickHouse. Error code: " + IntegerToString(res);

      //--- Try to get error details
      if(res > 0)
      {
         result = CharArrayToString(response);
         error_msg += ". Response: " + result;
      }
      else
      {
         //--- Specific error messages for common HTTP errors
         switch(res)
         {
            case -1:
               error_msg += ". Error: Failed to connect to ClickHouse server. Check host and port.";
               break;
            case 401:
               error_msg += ". Error: Authentication failed. Check username and password.";
               break;
            case 403:
               error_msg += ". Error: Permission denied. Check database permissions.";
               break;
            case 404:
               error_msg += ". Error: Database or table not found. Check database name.";
               break;
            case 500:
               error_msg += ". Error: ClickHouse server internal error.";
               break;
            default:
               error_msg += ". Error: Unknown HTTP error.";
               break;
         }
      }

      Print(error_msg);

      //--- If we can't connect to ClickHouse, disable resume functionality to avoid continuous errors
      if(res < 0 || res == 401 || res == 403 || res == 404)
      {
         Print("Disabling resume functionality due to connection error.");
         m_resume_enabled = false;
      }

      return false;
   }
}

//+------------------------------------------------------------------+
//| Get latest tick timestamp for a specific symbol                  |
//+------------------------------------------------------------------+
datetime GetLatestTickTimestampForSymbol(string symbol)
{
   //--- Check if we have latest timestamps
   if(m_symbol_latest_times.Total() == 0)
      return 0; // Return 0 if no data available

   //--- Search for the symbol in the array
   int total = m_symbol_latest_times.Total();
   for(int i = 0; i < total; i++)
   {
      string entry = m_symbol_latest_times.At(i);

      //--- Split entry to get symbol and time
      string parts[];
      StringSplit(entry, '|', parts);

      if(ArraySize(parts) == 2 && parts[0] == symbol)
      {
         //--- Parse the time string to datetime
         //--- Format: "YYYY-MM-DD HH:MM:SS.mmmmmmmmm"
         MqlDateTime time_struct;
         string time_str = parts[1];

         //--- Extract date parts
         string year_str = StringSubstr(time_str, 0, 4);
         string month_str = StringSubstr(time_str, 5, 2);
         string day_str = StringSubstr(time_str, 8, 2);

         //--- Extract time parts
         string hour_str = StringSubstr(time_str, 11, 2);
         string minute_str = StringSubstr(time_str, 14, 2);
         string second_str = StringSubstr(time_str, 17, 2);

         //--- Convert to integers
         time_struct.year = (int)StringToInteger(year_str);
         time_struct.mon = (int)StringToInteger(month_str);
         time_struct.day = (int)StringToInteger(day_str);
         time_struct.hour = (int)StringToInteger(hour_str);
         time_struct.min = (int)StringToInteger(minute_str);
         time_struct.sec = (int)StringToInteger(second_str);

         //--- Convert to datetime
         datetime result = StructToTime(time_struct);

         //--- Add 2 hours to convert from UTC+3 back to UTC+5 (reverse of what we do in FormatTickAsJson)
         result += 7200;

         return result;
      }
   }

   return 0; // Return 0 if symbol not found
}

//+------------------------------------------------------------------+