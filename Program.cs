using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Salesforce
{
    internal class Program
    {
        private static SalesforceClient CreateClient()
        {
            return new SalesforceClient
            {
                Username = ConfigurationManager.AppSettings["username"],
                Password = ConfigurationManager.AppSettings["password"],
                Token = ConfigurationManager.AppSettings["token"],
                ClientId = ConfigurationManager.AppSettings["clientId"],
                ClientSecret = ConfigurationManager.AppSettings["clientSecret"]
            };
        }

        public static DataTable FetchStudentDataIntoDataTable(string connectionString, string query)
        {
            DataTable dataTable = new DataTable();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    using (SqlDataAdapter dataAdapter = new SqlDataAdapter(command))
                    {
                        dataAdapter.Fill(dataTable);
                    }
                }
            }
            if (dataTable.Columns.Contains("TimestampColumn"))
            {
                dataTable.Columns.Remove("TimestampColumn");
            }
            return dataTable;
        }

        static string AddSuffixToColumnNames(string jsonData, string suffix)
        {
            // Parse the JSON data
            JArray jsonArray = JArray.Parse(jsonData);

            // Modify the column names in each item of the array
            foreach (JObject item in jsonArray)
            {
                var properties = item.Properties().ToList(); // To avoid modifying while iterating

                foreach (var property in properties)
                {
                    if (property.Name == "FirstName")
                    {
                        // Rename only "studName" to "Name"
                        item.Add(new JProperty("Name", property.Value));
                        item.Remove(property.Name);
                    }

                    else
                    {
                        // For other properties, add the suffix as usual
                        item.Remove(property.Name);
                        item.Add(new JProperty(property.Name + suffix, property.Value));
                    }
                }
            }

            // Convert the modified JSON array back to a string
            return jsonArray.ToString();
        }

        public static void Main()
        {
            var client = CreateClient();

            try
            {
                // client.logManager.InsertDataToLog("Main Method started");

                client.login();
                //client.DeleteAllRecords("Stud__c", "1");
                string viewQuery = "SELECT * FROM [dbo].[STU_VW]";
                DataTable syncDataTable = FetchStudentDataIntoDataTable(ConfigurationManager.AppSettings["ConnectionString"], viewQuery);

                if (syncDataTable.Rows.Count > 0)
                {
                    Console.WriteLine("Data has to be updated");
                    string connectionString = ConfigurationManager.AppSettings["ConnectionString"];
                    string query = "SELECT StudentID, FirstName, LastName, Gender, ContactNumber, Email, Address, DateOfBirth from [dbo].[STU_VW]";
                    DataTable dt = FetchStudentDataIntoDataTable(connectionString, query);
                   // Console.WriteLine(dt.ToString());
                    string jsonData = JsonConvert.SerializeObject(dt, Formatting.Indented);

                    string dataJson = AddSuffixToColumnNames(jsonData, "__c");

                    if (dataJson != null)
                    {
                        JArray jsonArray = JArray.Parse(dataJson);

                        // Iterate through each object in the array
                        foreach (JObject item in jsonArray)
                        {
                            Console.WriteLine();

                            string customObject = "Stud__c";
                            string sfdcId = client.Upsert(customObject, item.ToString());

                            // Update TimestampRepo with the current upsert time and SFDCI                    }
                        }


                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
