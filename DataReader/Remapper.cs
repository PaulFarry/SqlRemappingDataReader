using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Data
{
    public class Remapper
    {
        private long Index;
        public string ColumnName { get; set; }
        public bool PerformMapping { get; set; }
        public int ColumnIndex { get; set; }
        public Dictionary<long, long> Lookup { get; set; }
        internal ReMapType ReMapType { get; set; }

        public Remapper()
        {
            Lookup = new Dictionary<long, long>();
            Index = 0;
            ReMapType = ReMapType.None;
        }

        public long Remap(long value)
        {
            if (Lookup.ContainsKey(value)) return Lookup[value];

            Lookup.Add(value, ++Index);
            return Index;
        }
    }
    }
