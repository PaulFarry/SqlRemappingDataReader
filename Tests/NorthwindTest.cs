using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using NUnit.Framework;
using System.IO;
using Data;
using ProtoBuf.Data;

namespace Tests
{
    class NorthwindTest
    {

        private SqlConnectionStringBuilder csb;

        private Remapper remap;

        [TestFixtureSetUp]
        public void Setup()
        {
            //this.csb = new SqlConnectionStringBuilder() { DataSource = @".\idealsql", InitialCatalog = "Northwind", IntegratedSecurity = true, ConnectTimeout = 5 };
            this.csb = new SqlConnectionStringBuilder() { DataSource = @".\SQLExpress", InitialCatalog = "Northwind", IntegratedSecurity = true, ConnectTimeout = 5 };
            this.remap = new Remapper();
            this.remap.PerformMapping = true;
        }

        [Test]
        public void NormalTest()
        {
            this.remap.PerformMapping = false;

            using (MemoryStream ms = new MemoryStream())
            {
                using (SqlConnection conn = new SqlConnection(this.csb.ToString()))
                {
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "Select * from Orders where orderID >= 11000";

                        conn.Open();

                        using (SqlRemappingDataReader dr = new SqlRemappingDataReader(cmd.ExecuteReader(), this.remap))
                        {
                            DataSerializer.Serialize(ms, dr);
                        }
                    }
                }

                ms.Seek(0, SeekOrigin.Begin);

                using (var dr = DataSerializer.Deserialize(ms))
                {
                    while (dr.Read())
                    {
                        Console.WriteLine(dr["OrderID"].ToString());
                    }
                }
            }
        }

        [Test]
        public void RemapTest()
        {
            this.remap.PerformMapping = true;
            using (MemoryStream ms = new MemoryStream())
            {
                using (SqlConnection conn = new SqlConnection(this.csb.ToString()))
                {
                    using (SqlCommand cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "Select * from Orders where orderID >= 11000";

                        conn.Open();


                        this.remap.ColumnName = "orderid";

                        using (SqlRemappingDataReader dr = new SqlRemappingDataReader(cmd.ExecuteReader(), this.remap))
                        {
                            DataSerializer.Serialize(ms, dr);
                        }
                    }
                }

                ms.Seek(0, SeekOrigin.Begin);

                using (var dr = DataSerializer.Deserialize(ms))
                {
                    while (dr.Read())
                    {
                        Console.WriteLine(dr["OrderID"].ToString());
                    }
                }
            }
        }
    }
}
