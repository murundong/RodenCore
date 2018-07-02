using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RodenCore.Expand
{
    public static class StringExpand
    {
        public static string Replace(this string s,int index,int length,string replacement)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(s.Substring(0, index));
            sb.Append(replacement);
            sb.Append(s.Substring(index + length));
            return sb.ToString();
        }
    }
}
