using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json;
using System.Net;
using Newtonsoft.Json.Linq;
using LogLibrary;
using System.Configuration;
using System.Data.SqlClient;
namespace Salesforce
{
    internal class SalesforceClient
    {
        private const string LOGIN_ENDPOINT = "https://login.salesforce.com/services/oauth2/token";
        private const string API_ENDPOINT = "/services/data/v51.0";
        public Logger Logger { get; set; }

        public string Username { get; set; }
        public string Password { get; set; }
        public string Token { get; set; }
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
        public string refresh_token { get; set; }

        public string AuthToken { get; set; }
        public string InstanceUrl { get; set; }

        //public SalesforceClient()
        //{
        //    string connectionString = ConfigurationManager.AppSettings["ConnectionString"];
            
        //}
        static SalesforceClient()
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls;
        }

        public void login()
        {
            Logger.Log("Login Function Invoked");
            Console.WriteLine("Login Function Invoked");

            try
            {
                var clientId = ClientId;
                var clientSecret = ClientSecret;
                var username = Username;
                var password = Password + Token;

                var client = new HttpClient();
                var tokenRequest = new HttpRequestMessage(HttpMethod.Post, LOGIN_ENDPOINT);
                tokenRequest.Content = new FormUrlEncodedContent(new[]
                {
            new KeyValuePair<string, string>("grant_type", "password"),
            new KeyValuePair<string, string>("client_id", clientId),
            new KeyValuePair<string, string>("client_secret", clientSecret),
            new KeyValuePair<string, string>("username", username),
            new KeyValuePair<string, string>("password", password)
        });

                // Request the token
                var tokenResponse = client.SendAsync(tokenRequest).Result;
                var body = tokenResponse.Content.ReadAsStringAsync().Result;

                if (!tokenResponse.IsSuccessStatusCode)
                {
                    Console.WriteLine($"Error getting access token. Status Code: {tokenResponse.StatusCode}, Reason: {tokenResponse.ReasonPhrase}");
                    return;
                }

                var values = JsonConvert.DeserializeObject<Dictionary<string, string>>(body);

                if (values.ContainsKey("access_token"))
                {
                    AuthToken = values["access_token"];
                    Console.WriteLine("AuthToken = " + AuthToken);
                }
                else
                {
                    Console.WriteLine("Access token not found in the response.");
                    return;
                }

                if (values.ContainsKey("instance_url"))
                {
                    InstanceUrl = values["instance_url"];
                    Console.WriteLine("Instance URL = " + InstanceUrl);
                }
                else
                {
                    Console.WriteLine("Instance URL not found in the response.");
                }
            }
            catch (Exception ex)
            {
                Logger.Log($"An error occurred during login: {ex.Message}");
                Console.WriteLine($"An error occurred during login: {ex.Message}");
            }
        }
        public static void PrintJsonTable(string jsonResponse)
        {
            // Parse JSON
            var jsonObject = JObject.Parse(jsonResponse);

            // Extract records
            var records = jsonObject["records"];

            // Check if there are records
            if (records != null)
            {
                // Print header dynamically based on the first record
                var firstRecord = records.First;
                var properties = firstRecord.Children<JProperty>().Where(p => p.Name != "attributes"); // Exclude "attributes" property

                // Print header with fixed width
                Console.WriteLine(string.Join("\t", properties.Select(p => p.Name.PadRight(15))));

                // Print each record in table format with fixed width
                foreach (var record in records)
                {
                    Console.WriteLine(string.Join("\t", properties.Select(p => record[p.Name].ToString().PadRight(15))));
                }
            }
            else
            {
                Console.WriteLine("No records found.");
            }
        }
        public string Query(string soqlquery)
        {
            Logger.Log("Started to Query the table");

            using (var client = new HttpClient())
            {
                string restRequest = InstanceUrl + API_ENDPOINT + "/query?q=" + soqlquery;
                Console.WriteLine("REST Request URL: " + restRequest);
                var request = new HttpRequestMessage(HttpMethod.Get, restRequest);
                request.Headers.Add("Authorization", "Bearer " + AuthToken);
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                request.Headers.Add("X-PreetyPrint", "1");

                var response = client.SendAsync(request).Result;
                string jsonResponse = response.Content.ReadAsStringAsync().Result;
                return jsonResponse;
            }

        }

        public string Insert(string sObject, string recordData)
        {
            using (var client = new HttpClient())
            {
                string restRequest = InstanceUrl + API_ENDPOINT + "/sobjects/" + sObject + "/";
                //Console.WriteLine("REST Request URL: " + restRequest); // Debug statement

                // Log the record data before making the request
               // Console.WriteLine("Record Data:");
               // Console.WriteLine(recordData);

                var request = new HttpRequestMessage(HttpMethod.Post, restRequest);
                request.Headers.Add("Authorization", "Bearer " + AuthToken);
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                request.Headers.Add("X-PreetyPrint", "1");

                request.Content = new StringContent(recordData, Encoding.UTF8, "application/json");

                var response = client.SendAsync(request).Result;

                if (response.IsSuccessStatusCode)
                {
                    Logger.Log("Data Inserted Successfully");

                    // Extract Salesforce ID from the response
                    JObject jsonResponse = JObject.Parse(response.Content.ReadAsStringAsync().Result);
                    string sfdcId = jsonResponse.Value<string>("id");
                    Logger.Log($"SFCID is:{sfdcId}");
                    JObject recordObject = JObject.Parse(recordData);
                    string studentId = recordObject.Value<string>("StudentID__c");
                    Logger.Log($"studentid to be entered as key: {studentId}");
                    // Update TimestampRepo with the current upsert time and SFDCID
                    UpdateTimestampRepo(studentId, DateTime.Now, sfdcId);
                    return sfdcId;
                }
                else
                {
                    Logger.Log($"Error inserting data into {sObject}, status code: {response.StatusCode}, status phrase: {response.ReasonPhrase}");
                    Logger.Log($"Response content: {response.Content.ReadAsStringAsync().Result}");
                    return null;
                }
            }
        }




        public string Update(string sObject, string externalId, string recordData)
        {
            using (var client = new HttpClient())
            {
                string restRequest = $"{InstanceUrl}{API_ENDPOINT}/sobjects/{sObject}/Student_Id__c/{externalId}";
               // Console.WriteLine("REST Request URL: " + restRequest); // Debug statement

               // Console.WriteLine("Record Data: " + recordData); // Debug statement

                // Create the HTTP request
                var request = new HttpRequestMessage(HttpMethod.Patch, restRequest);
                request.Headers.Add("Authorization", "Bearer " + AuthToken);
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                request.Headers.Add("X-PrettyPrint", "1");

                // Set the request body with the JSON data
                request.Content = new StringContent(recordData, Encoding.UTF8, "application/json");

                // Send the request and get the response
                var response = client.SendAsync(request).Result;
                Logger.Log("Data Upserted Successfully");

                return response.Content.ReadAsStringAsync().Result;
            }
        }
        public void UpdateTimestampRepo(string primaryKeyValue, DateTime upsertTime, string sfdcId)
        {
            try
            {
                string connectionString = ConfigurationManager.AppSettings["ConnectionString"];

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    // Log the parameters for debugging
                    Console.WriteLine($"Updating TimestampRepo: Key = {primaryKeyValue}, UpsertTime = {upsertTime}, SFDCID = {sfdcId}");

                    // Check if the record with the specified Key exists
                    bool recordExists;
                    using (SqlCommand checkCommand = new SqlCommand("SELECT COUNT(*) FROM TimestampRepository WHERE [Key] = @PrimaryKeyValue", connection))
                    {
                        checkCommand.Parameters.AddWithValue("@PrimaryKeyValue", primaryKeyValue);
                        recordExists = (int)checkCommand.ExecuteScalar() > 0;
                    }

                    if (recordExists)
                    {
                        // Update the existing record in TimestampRepo
                        using (SqlCommand updateCommand = new SqlCommand("UPDATE TimestampRepository SET SavedTimeStamp = (SELECT TimestampColumn FROM Nirmal WHERE StudentID = @PrimaryKeyValue), UpSertTime = @UpsertTime, SFDCID = @SFDCID WHERE [Key] = @PrimaryKeyValue", connection))
                        {
                            updateCommand.Parameters.AddWithValue("@UpsertTime", upsertTime);
                            updateCommand.Parameters.AddWithValue("@SFDCID", sfdcId);
                            updateCommand.Parameters.AddWithValue("@PrimaryKeyValue", primaryKeyValue);

                            // Log the SQL command for debugging
                            Console.WriteLine($"Executing SQL command: {updateCommand.CommandText}");

                            int rowsAffected = updateCommand.ExecuteNonQuery();

                            Console.WriteLine($"Rows affected: {rowsAffected}");
                        }
                    }
                    else
                    {
                        // Insert a new record in TimestampRepo
                        using (SqlCommand insertCommand = new SqlCommand("INSERT INTO TimestampRepository ([Key], SavedTimeStamp, UpSertTime, SFDCID) VALUES (@PrimaryKeyValue, (SELECT TimestampColumn FROM Nirmal WHERE StudentID = @PrimaryKeyValue), @UpsertTime, @SFDCID)", connection))
                        {
                            insertCommand.Parameters.AddWithValue("@UpsertTime", upsertTime);
                            insertCommand.Parameters.AddWithValue("@SFDCID", sfdcId);
                            insertCommand.Parameters.AddWithValue("@PrimaryKeyValue", primaryKeyValue);

                            // Log the SQL command for debugging
                            Console.WriteLine($"Executing SQL command: {insertCommand.CommandText}");

                            int rowsAffected = insertCommand.ExecuteNonQuery();

                            Console.WriteLine($"Rows affected: {rowsAffected}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error updating/inserting TimestampRepo: {ex.Message}");
                // Handle the exception as needed
            }
        }


        public string Upsert(string sObject, string recordData)
        {
            // Parse the record data to get external ID
            JObject recordObject = JObject.Parse(recordData);
            string externalId = recordObject.Value<string>("StudentID__c");

            // Check if the record with the provided external ID exists
            string queryResult = Query($"SELECT Id FROM {sObject} WHERE StudentID__c = '{externalId}'");

            if (RecordExists(queryResult))
            {
                // Perform an update
                return Update(sObject, externalId, recordData);
            }
            else
            {
                // Perform an insert
                return Insert(sObject, recordData);
            }
        }

        private static bool RecordExists(string queryResult)
        {
            try
            {

                dynamic resultObject = JsonConvert.DeserializeObject(queryResult);

                if (resultObject.records != null && resultObject.records.Count > 0)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                Logger.Log($"Error parsing query result: {ex.Message}");
                Console.WriteLine("Error parsing query result: " + ex.Message);
                return false;
            }
        }




    }
}
