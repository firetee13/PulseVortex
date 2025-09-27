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
input int    InpMaxTicksPerBatch = 1000;            // Max Ticks Per Batch
input bool   InpDebugMode = false;                 // Debug Mode
input int    InpHistoricalTicksDays = 0;           // Historical Ticks Days (0 = no historical data)

//--- Global variables
CSymbolInfo m_symbol_info;                         // Symbol info object
CArrayString m_symbols;                            // Array of symbols to track
string m_ticks_data[];                             // Array to store tick data
datetime m_last_send_time = 0;                     // Last send time
int m_http_timeout = 5000;                         // HTTP timeout in milliseconds
bool m_symbols_subscribed = false;                 // Flag to check if symbols are subscribed

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

   //--- Get historical ticks if requested
   if(InpHistoricalTicksDays > 0)
   {
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

   //--- Unsubscribe from all symbols
   UnsubscribeFromAllSymbols();

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
      if(SymbolSelect(symbol, false))
      {
         if(InpDebugMode)
            Print("Unsubscribed from symbol: ", symbol);
      }
   }

   m_symbols_subscribed = false;
   Print("Unsubscribed from all symbols.");
}

//+------------------------------------------------------------------+
//| Get historical ticks for all symbols                             |
//+------------------------------------------------------------------+
bool GetHistoricalTicks()
{
   //--- Check if we need to get historical ticks
   if(InpHistoricalTicksDays <= 0)
   {
      if(InpDebugMode)
         Print("Historical ticks retrieval disabled (InpHistoricalTicksDays = ", InpHistoricalTicksDays, ")");
      return true;
   }

   //--- Calculate start time (current time - specified days)
   datetime end_time = TimeCurrent();
   datetime start_time = end_time - (InpHistoricalTicksDays * 24 * 60 * 60);

   if(InpDebugMode)
   {
      Print("Retrieving historical ticks from ", TimeToString(start_time), " to ", TimeToString(end_time));
      Print("Total symbols to process: ", m_symbols.Total());
   }

   int total_symbols = m_symbols.Total();
   int total_ticks_retrieved = 0;

   //--- Process each symbol
   for(int i = 0; i < total_symbols; i++)
   {
      string symbol = m_symbols.At(i);

      if(InpDebugMode)
         Print("Processing historical ticks for symbol: ", symbol);

      //--- Get historical ticks for this symbol
      MqlTick ticks_array[];
      // Explicitly use the datetime range overload of CopyTicks
      int ticks_count = CopyTicks(symbol, ticks_array, COPY_TICKS_ALL, (datetime)start_time, (datetime)end_time);

      if(ticks_count <= 0)
      {
         if(InpDebugMode)
            Print("No historical ticks found for symbol: ", symbol, ". Error: ", GetLastError());
         continue;
      }

      if(InpDebugMode)
         Print("Retrieved ", ticks_count, " historical ticks for symbol: ", symbol);

      //--- Process each tick
      int accepted_count = 0;
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
         accepted_count++;

         //--- Send batch if it's full (accepted_count counts only ticks that were added)
         if((accepted_count) % InpMaxTicksPerBatch == 0)
         {
            SendTicksToClickHouse();

            //--- Small delay to avoid overwhelming the server
            Sleep(100);
         }
      }

      //--- Send any remaining ticks for this symbol
      SendTicksToClickHouse();

      //--- Small delay between symbols
      Sleep(200);
   }

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