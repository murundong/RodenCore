using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace RodenCore.Common
{
    public class HelperSecure
    {
      
        /// <summary>
        /// MD5加密
        /// </summary>
        /// <param name="str">加密字符串</param>
        /// <param name="encoding">编码</param>
        /// <param name="toUpper">是否转换成大写</param>
        /// <returns></returns>
        public static string MD5Encrypt(string str,Encoding encoding,bool toUpper=false)
        {
            MD5CryptoServiceProvider md5 = new MD5CryptoServiceProvider();
            byte[] encryptedBytes = md5.ComputeHash(encoding.GetBytes(str));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < encryptedBytes.Length; i++)
            {
                sb.Append(encryptedBytes[i].ToString("x2"));
            }
            return  toUpper?sb.ToString().ToUpper():sb.ToString();
        }


        /// <summary>
        /// Des对称加密
        /// </summary>
        /// <param name="key">密钥</param>
        /// <param name="iv">种子</param> 
        /// <param name="val"></param>
        /// <param name="encoding">编码方式</param>
        /// <returns></returns>
        public static string DesEncrypt(string key, string iv, string val, Encoding encoding)
        {
            try
            {
                DESCryptoServiceProvider des = new DESCryptoServiceProvider();
                des.Key = encoding.GetBytes(key);
                des.IV = encoding.GetBytes(iv);
                ICryptoTransform transform = des.CreateEncryptor();
                byte[] buffer = encoding.GetBytes(val);
                byte[] result = transform.TransformFinalBlock(buffer, 0, buffer.Length);
                return Convert.ToBase64String(result);
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// DES对称解密
        /// </summary>
        /// <param name="key">密钥</param>
        /// <param name="iv">种子</param>
        /// <param name="val"></param>
        /// <param name="encoding">编码方式</param>
        /// <returns></returns>
        public static string DesDecrypt(string key, string iv, string val, Encoding encoding)
        {
            try
            {
                DESCryptoServiceProvider des = new DESCryptoServiceProvider();
                des.Key = encoding.GetBytes(key);
                des.IV = encoding.GetBytes(iv);
                ICryptoTransform transform = des.CreateDecryptor();
                byte[] buffer = Convert.FromBase64String(val.Replace(" ", "+"));//去掉UrlDecode可能导致的转义问题
                byte[] result = transform.TransformFinalBlock(buffer, 0, buffer.Length);
                return Encoding.UTF8.GetString(result);
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}
