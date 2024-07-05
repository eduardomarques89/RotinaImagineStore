using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using RestSharp.Serializers;

namespace RotinaImagineStore
{
    internal class Program
    {
        static void Main(string[] args)
        {
            GETVendas();
            //GETFornecedores();
            //GETProdutos();
        }

        public static void GETVendas()
        {
            JsonConversao jsonconv = new JsonConversao();
            var client = new RestClient("https://vmpay.vertitecnologia.com.br/api/v1");
            int page = 1;
            int perPage = 1000;
            int maxPage = 7;

            while (page <= maxPage)
            {
                var request = new RestRequest($"/cashless_facts?access_token=04PJ5nF3VnLIfNLJRbqmZkEMhU2VNCClOjPoTPCI&start_date=2024-07-03&end_date=2024-07-09&page={page}&per_page={perPage}");

                request.AddHeader("Accept", "application/json");

                RestResponse response = client.ExecuteGet(request);

                if (response.StatusCode != System.Net.HttpStatusCode.OK)
                {
                    Console.WriteLine("Erro na requisição: " + response.StatusDescription);
                    return;
                }

                dynamic resultado;
                try
                {
                    resultado = JArray.Parse(response.Content);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erro ao converter JSON: {ex.Message}");
                    return;
                }

                try
                {
                    using (SqlConnection conn_ds = new SqlConnection("Server=tcp:imaginestore.database.windows.net,1433;Database=imagine;User Id=imaginestore;Password=Sqlis@23is;"))
                    {
                        conn_ds.Open();

                        foreach (var venda in resultado)
                        {
                            string id = venda["id"].ToString();
                            string status = venda["status"].ToString();
                            string occurred_at = venda["occurred_at"].ToString().Replace("T", " ").Replace("Z", "");
                            string point_of_sale = venda["point_of_sale"].ToString();
                            string equipment_id = venda["equipment_id"].ToString();
                            string installation_id = venda["installation_id"].ToString();
                            string planogram_item_id = venda["planogram_item_id"].ToString();
                            string equipment_label_number = venda["equipment_label_number"].ToString();
                            string equipment_serial_number = venda["equipment_serial_number"].ToString();
                            string masked_card_number = venda["masked_card_number"]?.ToString() ?? "";
                            string number_of_payments = venda["number_of_payments"]?.ToString() ?? "";
                            string quantity = venda["quantity"].ToString();
                            string value = venda["value"].ToString();
                            string request_number = venda["request_number"]?.ToString() ?? "";
                            string issuer_authorization_code = venda["issuer_authorization_code"]?.ToString() ?? "";
                            string client_id = venda["client"]?["id"]?.ToString() ?? "";
                            string client_name = venda["client"]?["name"]?.ToString() ?? "";
                            string location_id = venda["location"]?["id"]?.ToString() ?? "";
                            string location_name = venda["location"]?["name"]?.ToString() ?? "";
                            string machine_id = venda["machine"]?["id"]?.ToString() ?? "";
                            string machine_model_id = venda["machine_model"]?["id"]?.ToString() ?? "";
                            string machine_model_name = venda["machine_model"]?["name"]?.ToString() ?? "";
                            string good_id = venda["good"]?["id"]?.ToString() ?? "";
                            string good_type = venda["good"]?["type"]?.ToString() ?? "";
                            string good_category_id = venda["good"]?["category_id"]?.ToString() ?? "";
                            string good_name = venda["good"]?["name"]?.ToString() ?? "";
                            string manufacturer_id = venda["good"]?["manufacturer_id"].ToString();
                            string upc_code = venda["good"]?["upc_code"]?.ToString() ?? "";
                            string barcode = venda["good"]?["barcode"].ToString();
                            string eft_provider_id = venda["eft_provider"]?["name"]?.ToString() ?? "";
                            string eft_authorizer_id = venda["eft_authorizer"]?["name"]?.ToString() ?? "";
                            string eft_card_brand_id = venda["eft_card_brand"]?["name"]?.ToString() ?? "";
                            string eft_card_type_id = venda["eft_card_type"]?["name"]?.ToString() ?? "";
                            string payment_authorizer = venda["payment_authorizer"]?["name"]?.ToString() ?? "";

                            using (SqlCommand selectCommand = new SqlCommand("SELECT * FROM vendas WHERE id = @id", conn_ds))
                            {
                                selectCommand.Parameters.AddWithValue("@id", id);

                                if (venda == null || venda["client"] == null || venda["location"] == null)
                                {
                                    Console.WriteLine("Objeto de venda ou suas propriedades estão nulos.");
                                    continue;
                                }

                                //int count = (int)selectCommand.ExecuteScalar();

                                if (/*count > 0 ||*/ string.IsNullOrEmpty(manufacturer_id))
                                {
                                    Console.WriteLine($"A venda com ID '{id}' já existe na base de dados ou o 'manufacturer_id' está vazio.");
                                    continue;
                                }
                                try
                                {
                                    using (SqlCommand insertCommand = new SqlCommand(
                                        "INSERT INTO vendas (id, occurred_at, client_id, location_id, machine_id, installation_id, planogram_item_id, good_id, coil, quantity, value, client_name, location_name, machine_model_name, type, " +
                                        "category_id, manufacturer_id, product_name, upc_code, barcode, point_of_sale, equipment_id, equipment_label_number, equipment_serial_number, masked_card_number, number_of_payments, request_number, " +
                                        "issuer_authorization_code, machine_model, planogram_item, eft_provider, eft_authorizer, eft_card_brand, eft_card_type, payment_authorizer, status, data_criacao) VALUES " +
                                        "(@id, @occurred_at, @client_id, @location_id, @machine_id, @installation_id, @planogram_item_id, @good_id, @coil, @quantity, @value, @client_name, @location_name, @machine_model_name, @type, " +
                                        "@category_id, @manufacturer_id, @product_name, @upc_code, @barcode, @point_of_sale, @equipment_id, @equipment_label_number, @equipment_serial_number, @masked_card_number, @number_of_payments, @request_number, " +
                                        "@issuer_authorization_code, @machine_model, @planogram_item, @eft_provider, @eft_authorizer, @eft_card_brand, @eft_card_type, @payment_authorizer, @status, GETDATE())", conn_ds))
                                    {
                                        insertCommand.Parameters.AddWithValue("@id", id);
                                        insertCommand.Parameters.AddWithValue("@occurred_at", Convert.ToDateTime(occurred_at));
                                        insertCommand.Parameters.AddWithValue("@client_id", client_id);
                                        insertCommand.Parameters.AddWithValue("@location_id", location_id);
                                        insertCommand.Parameters.AddWithValue("@machine_id", machine_id);
                                        insertCommand.Parameters.AddWithValue("@installation_id", installation_id);
                                        insertCommand.Parameters.AddWithValue("@planogram_item_id", planogram_item_id);
                                        insertCommand.Parameters.AddWithValue("@good_id", good_id);
                                        insertCommand.Parameters.AddWithValue("@coil", barcode);
                                        insertCommand.Parameters.AddWithValue("@quantity", quantity);
                                        insertCommand.Parameters.AddWithValue("@value", Convert.ToDouble(value));
                                        insertCommand.Parameters.AddWithValue("@client_name", client_name);
                                        insertCommand.Parameters.AddWithValue("@location_name", location_name);
                                        insertCommand.Parameters.AddWithValue("@machine_model_name", machine_model_id);
                                        insertCommand.Parameters.AddWithValue("@type", good_type);
                                        insertCommand.Parameters.AddWithValue("@category_id", good_category_id);
                                        insertCommand.Parameters.AddWithValue("@manufacturer_id", manufacturer_id);
                                        insertCommand.Parameters.AddWithValue("@product_name", good_name);
                                        insertCommand.Parameters.AddWithValue("@upc_code", upc_code);
                                        insertCommand.Parameters.AddWithValue("@barcode", barcode);
                                        insertCommand.Parameters.AddWithValue("@point_of_sale", point_of_sale);
                                        insertCommand.Parameters.AddWithValue("@equipment_id", equipment_id);
                                        insertCommand.Parameters.AddWithValue("@equipment_label_number", equipment_label_number);
                                        insertCommand.Parameters.AddWithValue("@equipment_serial_number", equipment_serial_number);
                                        insertCommand.Parameters.AddWithValue("@masked_card_number", masked_card_number);
                                        insertCommand.Parameters.AddWithValue("@number_of_payments", number_of_payments);
                                        insertCommand.Parameters.AddWithValue("@request_number", request_number);
                                        insertCommand.Parameters.AddWithValue("@issuer_authorization_code", issuer_authorization_code);
                                        insertCommand.Parameters.AddWithValue("@machine_model", machine_model_name);
                                        insertCommand.Parameters.AddWithValue("@planogram_item", planogram_item_id);
                                        insertCommand.Parameters.AddWithValue("@eft_provider", eft_provider_id);
                                        insertCommand.Parameters.AddWithValue("@eft_authorizer", eft_authorizer_id);
                                        insertCommand.Parameters.AddWithValue("@eft_card_brand", eft_card_brand_id);
                                        insertCommand.Parameters.AddWithValue("@eft_card_type", eft_card_type_id);
                                        insertCommand.Parameters.AddWithValue("@payment_authorizer", payment_authorizer);
                                        insertCommand.Parameters.AddWithValue("@status", status);

                                        insertCommand.ExecuteNonQuery();
                                    }
                                    Console.WriteLine("Venda carregada!");
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"Erro ao inserir venda com ID '{id}': {ex.Message}");
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erro ao acessar o banco de dados: {ex.Message}");
                }

                page++;
            }

            Console.WriteLine("Processamento concluído!");
        }

        public static void GETFornecedores()
        {
            JsonConversao jsonconv = new JsonConversao();

            var client = new RestClient("https://vmpay.vertitecnologia.com.br/api/v1");
            var request = new RestRequest("/manufacturers?access_token=04PJ5nF3VnLIfNLJRbqmZkEMhU2VNCClOjPoTPCI");
            request.AddHeader("Accept", "application/json");

            RestResponse response = client.ExecuteGet(request);

            if (response.StatusCode != System.Net.HttpStatusCode.OK)
            {
                Console.WriteLine("Erro na requisição: " + response.StatusDescription);
                return;
            }

            dynamic resultado;
            try
            {
                resultado = JArray.Parse(response.Content);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao converter JSON: {ex.Message}");
                return;
            }

            foreach (var info in resultado)
            {
                string id = info["id"].ToString();
                string name = info["name"].ToString();

                try
                {
                    using (SqlConnection conn_ds = new SqlConnection("Server=tcp:imaginestore.database.windows.net,1433;Database=imagine;User Id=imaginestore;Password=Sqlis@23is;"))
                    {
                        conn_ds.Open();
                        try
                        {
                            using (SqlCommand insertCommand = new SqlCommand("SELECT * from fornecedores where id = '" + id + "'", conn_ds))
                            {
                                insertCommand.Parameters.AddWithValue("@id", id);
                                int count = (int)insertCommand.ExecuteScalar();

                                try
                                {
                                    if (count > 0)
                                    {
                                        using (SqlCommand updateCmd = new SqlCommand("UPDATE fornecedores SET name = @name WHERE id = @id", conn_ds))
                                        {
                                            updateCmd.Parameters.AddWithValue("@name", name);
                                            updateCmd.Parameters.AddWithValue("@id", id);
                                            updateCmd.ExecuteNonQuery();

                                            Console.WriteLine("Update Feito!");
                                        }
                                    }
                                    else
                                    {
                                        using (SqlCommand insertCmd = new SqlCommand("INSERT INTO fornecedores (id, name) VALUES (@id, @name)", conn_ds))
                                        {
                                            insertCmd.Parameters.AddWithValue("@id", id);
                                            insertCmd.Parameters.AddWithValue("@name", name);
                                            insertCmd.ExecuteNonQuery();

                                            Console.WriteLine("Inseriu Feito!");
                                        }
                                    }
                                    Console.WriteLine("Funcionou");
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine("Erro ao inserir: " + ex);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Erro ao carregar banco: " + ex);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Erro ao conectar ao banco: " + ex);
                }
            }
        }

        public static void GETProdutos()
        {
            JsonConversao jsonconv = new JsonConversao();

            using (SqlConnection conn = new SqlConnection("Server=tcp:imaginestore.database.windows.net,1433;Database=imagine;User Id=imaginestore;Password=Sqlis@23is;"))
            {
                conn.Open();

                using (SqlCommand cmd = new SqlCommand("SELECT distinct v.good_id, coil from vendas v left join produtos p on p.id = v.good_id where p.id is null", conn))
                {
                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var client = new RestClient("https://vmpay.vertitecnologia.com.br/api/v1");
                            var barcodes = reader["coil"].ToString();
                            var request = new RestRequest($"/products?access_token=04PJ5nF3VnLIfNLJRbqmZkEMhU2VNCClOjPoTPCI&barcode={barcodes}");
                            request.AddHeader("Accept", "application/json");

                            RestResponse response = client.Execute(request);

                            if (response.StatusCode != System.Net.HttpStatusCode.OK)
                            {
                                Console.WriteLine($"Erro na requisição: {response.StatusDescription}");
                                continue;
                            }

                            dynamic resultado;
                            try
                            {
                                resultado = JArray.Parse(response.Content);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"Erro ao converter JSON: {ex.Message}");
                                continue;
                            }

                            foreach (var info in resultado)
                            {
                                try
                                {
                                    string id = info["id"].ToString();
                                    string name = info["name"].ToString();
                                    string type = info["type"].ToString();
                                    string manufacturer_id = info["manufacturer_id"].ToString();
                                    string category_id = info["category_id"].ToString();
                                    string upc_code = info["upc_code"].ToString();
                                    string barcode = info["barcode"]?.ToString() ?? "";
                                    string default_price = info["default_price"]?.ToString() ?? "0";
                                    string image = info["image"]?.ToString() ?? "";

                                    double cost_price;
                                    if (!double.TryParse(default_price, out cost_price))
                                    {
                                        cost_price = 0;
                                    }


                                    using (SqlCommand insertCmd = new SqlCommand("INSERT INTO produtos (id, name, type, manufacturer_id, category_id, upc_code, barcode, default_price, image, create_date) VALUES (@id, @name, @type, @manufacturer_id, @category_id, @upc_code, @barcode, @default_price, @image, GETDATE())", conn))
                                    {
                                        insertCmd.Parameters.AddWithValue("@id", id);
                                        insertCmd.Parameters.AddWithValue("@name", name);
                                        insertCmd.Parameters.AddWithValue("@type", type);
                                        insertCmd.Parameters.AddWithValue("@manufacturer_id", manufacturer_id);
                                        insertCmd.Parameters.AddWithValue("@category_id", category_id);
                                        insertCmd.Parameters.AddWithValue("@upc_code", upc_code);
                                        insertCmd.Parameters.AddWithValue("@barcode", barcode);
                                        insertCmd.Parameters.AddWithValue("@default_price", cost_price);
                                        insertCmd.Parameters.AddWithValue("@image", image);

                                        int contadorDeErros = 0;
                                        try
                                        {
                                            insertCmd.ExecuteNonQuery();
                                        }
                                        catch (Exception ex)
                                        {
                                            contadorDeErros++;
                                            Console.WriteLine($"Erro {contadorDeErros} ao inserir produto no banco de dados: {ex.Message}");
                                        }
                                    }
                                }
                                catch (Exception ex)
                                {
                                    Console.WriteLine($"Erro ao processar produto: {ex.Message}");
                                }

                                reader.Close();
                            }
                        }
                    }
                }
            }
        }




        public class JsonConversao
        {

            public string ConverteObjectParaJSon<T>(T obj)
            {
                try
                {
                    DataContractJsonSerializer ser = new DataContractJsonSerializer(typeof(T));
                    MemoryStream ms = new MemoryStream();
                    ser.WriteObject(ms, obj);
                    string jsonString = Encoding.UTF8.GetString(ms.ToArray());
                    ms.Close();
                    return jsonString;
                }
                catch
                {
                    throw;
                }
            }
            public T ConverteJSonParaObject<T>(string jsonString)
            {
                try
                {
                    DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(T));
                    MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(jsonString));
                    T obj = (T)serializer.ReadObject(ms);
                    return obj;
                }
                catch
                {
                    throw;
                }
            }
        }
    } 
}
