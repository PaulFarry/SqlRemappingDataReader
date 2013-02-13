using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;

namespace Data
{
    public class SqlRemappingDataReader : IDataReader
    {

        protected IDataReader dr;
        protected Remapper remap;
        protected bool mappingEnabled;

        public SqlRemappingDataReader(SqlDataReader dr)
        {
            this.dr = dr;
            Remapper remap = new Remapper();
            mappingEnabled = false;
        }

        public SqlRemappingDataReader(SqlDataReader dr, Remapper remap)
            : this(dr)
        {
            this.remap = remap;
            mappingEnabled = true;
            ProcessHeader();
        }

        private void ProcessHeader()
        {
            if (!mappingEnabled) return;
            using (var schema = dr.GetSchemaTable())
            {
                var schemaSupportsExpressions = schema.Columns.Contains("Expression");

                bool foundColumn = false;

                if (this.remap.PerformMapping)
                {
                    for (var i = 0; i < schema.Rows.Count; i++)
                    {
                        // Assumption: rows in the schema table are always ordered by
                        // Ordinal position, ascending
                        var row = schema.Rows[i];

                        string ColumnName = (string)row["ColumnName"];

                        if (string.Equals(remap.ColumnName, ColumnName, StringComparison.OrdinalIgnoreCase))
                        {
                            remap.ColumnIndex = i;
                            foundColumn = true;
                            Type t = (Type)row["DataType"];
                            if (t == typeof(int))
                            {
                                remap.ReMapType = ReMapType.Int32;
                            }
                            else
                            {
                                if (t == typeof(long))
                                {
                                    remap.ReMapType = ReMapType.Int64;
                                }
                                else
                                {
                                    if (t == typeof(short))
                                    {
                                        remap.ReMapType = ReMapType.Int16;
                                    }
                                    else
                                    {
                                        if (t == typeof(byte))
                                        {
                                            remap.ReMapType = ReMapType.Byte;
                                        }
                                    }
                                }
                            }

                           break;
                        }
                    }
                }
                remap.PerformMapping = foundColumn;
            }
        }


        public bool NextResult()
        {
            ProcessHeader();
            return this.dr.NextResult();
        }


        public byte GetByte(int i)
        {
            if ((mappingEnabled) && (i == remap.ColumnIndex && remap.PerformMapping))
            {
                return (byte)remap.Remap((long)(this.dr.GetByte(i)));
            }
            return this.dr.GetByte(i);
        }

        public short GetInt16(int i)
        {
            if ((mappingEnabled) && (i == remap.ColumnIndex && remap.PerformMapping))
            {
                return (short)remap.Remap((long)(this.dr.GetInt16(i)));
            }
            return this.dr.GetInt16(i);
        }

        public int GetInt32(int i)
        {
            if ((mappingEnabled) && (i == remap.ColumnIndex && remap.PerformMapping))
            {
                return (int)remap.Remap((long)(this.dr.GetInt32(i)));
            }
            return this.dr.GetInt32(i);
        }

        public long GetInt64(int i)
        {
            if ((mappingEnabled) && (i == remap.ColumnIndex && remap.PerformMapping))
            {
                return remap.Remap((this.dr.GetInt64(i)));
            }
            return this.dr.GetInt64(i);
        }

        public object this[int i]
        {
            get
            {
                if (mappingEnabled && (remap.ColumnIndex == i) && remap.PerformMapping)
                {
                    switch (remap.ReMapType)
                    {
                        case ReMapType.Byte:
                            {
                                return (byte)remap.Remap(dr.GetByte(i));
                            }
                        case ReMapType.Int16:
                            {
                                return (short)remap.Remap(dr.GetInt16(i));
                            }
                        case ReMapType.Int32:
                            {
                                return (int)remap.Remap(dr.GetInt32(i));
                            }
                        case ReMapType.Int64:
                            {
                                return remap.Remap((long)dr[i]);
                                //return (long)mappedValue;
                            }
                        default:
                            {
                                break;
                            }
                    }
                }

                return this.dr[i];
            }
        }

        #region Standard Decoration of DataReader

        public void Close()
        {
            this.dr.Close();
        }

        public int Depth
        {
            get { return this.dr.Depth; }
        }

        public DataTable GetSchemaTable()
        {
            return this.dr.GetSchemaTable();
        }

        public bool IsClosed
        {
            get { return this.dr.IsClosed; }
        }

        public bool Read()
        {
            return this.dr.Read();
        }

        public int RecordsAffected
        {
            get { return this.dr.RecordsAffected; }
        }

        public void Dispose()
        {
            this.dr.Dispose();
        }

        public int FieldCount
        {
            get { return this.dr.FieldCount; }
        }

        public bool GetBoolean(int i)
        {
            return this.dr.GetBoolean(i);
        }


        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            return this.dr.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
        }

        public char GetChar(int i)
        {
            return this.dr.GetChar(i);
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            return this.dr.GetChars(i, fieldoffset, buffer, bufferoffset, length);
        }

        public IDataReader GetData(int i)
        {
            return this.dr.GetData(i);
        }

        public string GetDataTypeName(int i)
        {
            return this.dr.GetDataTypeName(i);
        }

        public DateTime GetDateTime(int i)
        {
            return this.dr.GetDateTime(i);
        }

        public decimal GetDecimal(int i)
        {
            return this.dr.GetDecimal(i);
        }

        public double GetDouble(int i)
        {
            return this.dr.GetDouble(i);
        }

        public Type GetFieldType(int i)
        {
            return this.dr.GetFieldType(i);
        }

        public float GetFloat(int i)
        {
            return this.dr.GetFloat(i);
        }

        public Guid GetGuid(int i)
        {
            return this.dr.GetGuid(i);
        }

        public string GetName(int i)
        {
            return this.dr.GetName(i);
        }

        public int GetOrdinal(string name)
        {
            return this.dr.GetOrdinal(name);
        }

        public string GetString(int i)
        {
            return this.dr.GetString(i);
        }

        public object GetValue(int i)
        {
            return this.dr.GetValue(i);
        }

        public int GetValues(object[] values)
        {
            return this.dr.GetValues(values);
        }

        public bool IsDBNull(int i)
        {
            return this.dr.IsDBNull(i);
        }

        public object this[string name]
        {
            get { return this.dr[name]; }
        }
        #endregion


    }
}
