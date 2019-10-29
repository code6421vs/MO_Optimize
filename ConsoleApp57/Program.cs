using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Diagnostics;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Internal;
using System.Data;
using System.Reflection.Emit;
using System.Reflection;

namespace ConsoleApp57
{
    public static class RowMappingHandler
    {
        static RowMappingHandler()
        {
        }


        public static IEnumerable<T> FromSQLRaw5<T>(this IDbConnection conn, string sql) where T : class, new()
        {
            conn.Open();
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                using var dr = cmd.ExecuteReader(CommandBehavior.CloseConnection | CommandBehavior.SequentialAccess);
                Dictionary<string, Delegate> mappedType;
                if (!_mappingCache.TryGetValue(typeof(T), out mappedType))
                {
                    _mappingCache.Add(typeof(T), new Dictionary<string, Delegate>());
                    mappedType = _mappingCache[typeof(T)];
                }
                Dictionary<PropertyInfo, int> fieldMap = null;
                bool mapRoundDown = false;
                while (dr.Read())
                {
                    var instance = new T();
                    if (fieldMap == null)
                        fieldMap = typeof(T).GetProperties().Select(a => new
                        { PropInfo = a, Index = GetFieldIndex(a.Name, dr) }).Where(a => a.Index != -1).OrderBy(a => a.Index).ToDictionary(a => a.PropInfo, v => v.Index);

                    foreach (var pi in fieldMap)
                    {
                        Delegate func;
                        var valueIndex = fieldMap[pi.Key];
                        if (!mapRoundDown && !mappedType.ContainsKey(pi.Key.Name))
                        {
                            func = CreateTo2<T>(pi.Key.Name);
                            mappedType[pi.Key.Name] = func;
                        }
                        else
                            func = mappedType[pi.Key.Name];

                        if (pi.Key.PropertyType == typeof(int))
                            ((Action<T, int>)func)(instance, dr.IsDBNull(valueIndex) ? -1 : dr.GetInt32(valueIndex));
                        else if (pi.Key.PropertyType == typeof(string))
                            ((Action<T, string>)func)(instance, dr.IsDBNull(valueIndex) ? (string)null : dr.GetString(valueIndex));
                    }
                    mapRoundDown = true;
                    yield return instance;
                }
            }
            finally
            {
                conn.Close();
            }
        }

        private static Delegate CreateTo2<TType>(string property)
        {
            var type = typeof(TType);
            var pi = type.GetProperty(property);
            if (pi == null)
                throw new ArgumentException($"property {property} not exists");
            var method = new DynamicMethod($"{type.FullName}_{property}_Setter", typeof(void), new[] { type, pi.PropertyType }, typeof(RowMappingHandler));
            var ilgen = method.GetILGenerator();
            ilgen.Emit(OpCodes.Ldarg_0);
            ilgen.Emit(OpCodes.Ldarg_1);
            if (pi.PropertyType.IsValueType)
                ilgen.Emit(OpCodes.Call, pi.GetSetMethod());
            else
                ilgen.Emit(OpCodes.Callvirt, pi.GetSetMethod());

            ilgen.Emit(OpCodes.Ret);
            return method.CreateDelegate(typeof(Action<,>).MakeGenericType(new[] { type, pi.PropertyType }));
        }
        

        public static IEnumerable<T> FromSQLRaw4<T>(this IDbConnection conn, string sql) where T : class, new()
        {
            List<T> result = new List<T>();
            conn.Open();
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                using var dr = cmd.ExecuteReader(CommandBehavior.CloseConnection | CommandBehavior.SequentialAccess);
                Dictionary<string, Delegate> mappedType;
                if (!_mappingCache.TryGetValue(typeof(T), out mappedType))
                {
                    _mappingCache.Add(typeof(T), new Dictionary<string, Delegate>());
                    mappedType = _mappingCache[typeof(T)];
                }
                Dictionary<PropertyInfo, int> fieldMap = null;
                bool mapRoundDown = false;
                while (dr.Read())
                {
                    var instance = new T();
                    if (fieldMap == null)
                        fieldMap = typeof(T).GetProperties().Select(a => new
                        { PropInfo = a, Index = GetFieldIndex(a.Name, dr) }).Where(a => a.Index != -1).OrderBy(a => a.Index).ToDictionary(a => a.PropInfo, v => v.Index) ;

                    foreach (var pi in fieldMap)
                    {
                        Delegate func;
                        if (!mapRoundDown && !mappedType.ContainsKey(pi.Key.Name))
                        {
                            func = CreateTo2<T>(pi.Key.Name);
                            mappedType[pi.Key.Name] = func;
                        }
                        else
                            func = mappedType[pi.Key.Name];

                        if (pi.Key.PropertyType == typeof(int))
                            ((Action<T, int>)func)(instance, dr.IsDBNull(fieldMap[pi.Key]) ? -1 : dr.GetInt32(fieldMap[pi.Key]));
                        else if (pi.Key.PropertyType == typeof(string))
                            ((Action<T, string>)func)(instance, dr.IsDBNull(fieldMap[pi.Key]) ? (string)null : dr.GetString(fieldMap[pi.Key]));
                    }
                    mapRoundDown = true;
                    result.Add(instance);
                }
            }
            finally
            {
                conn.Close();
            }
            return result;
        }

        private static Dictionary<Type, Dictionary<string, Delegate>> _mappingCache = new Dictionary<Type, Dictionary<string, Delegate>>();
        public static IEnumerable<T> FromSQLRaw3<T>(this IDbConnection conn, string sql) where T : class, new()
        {
            List<T> result = new List<T>();
            conn.Open();
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                using var dr = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection);
                Dictionary<string, Delegate> mappedType;
                if (!_mappingCache.TryGetValue(typeof(T), out mappedType))
                {
                    _mappingCache.Add(typeof(T), new Dictionary<string, Delegate>());
                    mappedType = _mappingCache[typeof(T)];
                }
                Dictionary<string, int> fieldMap = null;
                while (dr.Read())
                {
                    var instance = new T();
                    if (fieldMap == null)
                        fieldMap = typeof(T).GetProperties().Select(a => new
                        { a.Name, Index = GetFieldIndex(a.Name, dr) }).Where(a => a.Index != -1).OrderBy(a => a.Index).ToDictionary(a => a.Name, v => v.Index);
                        
                    var values = new object[dr.FieldCount];
                    dr.GetValues(values);
                    foreach (var pi in fieldMap)
                    {
                        if (!mappedType.ContainsKey(pi.Key))
                            mappedType[pi.Key] = CreateTo<T>(pi.Key);
                        ((Action<T, object>)mappedType[pi.Key])(instance, values[fieldMap[pi.Key]] == DBNull.Value ? null : values[fieldMap[pi.Key]]);
                    }
                    result.Add(instance);
                }
            }
            finally
            {
                conn.Close();
            }
            return result;
        }

        public static IEnumerable<T> FromSQLRaw<T>(this IDbConnection conn, string sql) where T : class, new()
        {
            List<T> result = new List<T>();
            conn.Open();
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                using var dr = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection);
                while (dr.Read())
                {
                    var instance = new T();
                    var values = new object[dr.FieldCount];
                    dr.GetValues(values);
                    foreach (var prop in typeof(T).GetProperties())
                    {
                        try
                        {
                            var fldIndex = dr.GetOrdinal(prop.Name);
                            prop.SetValue(instance, dr.IsDBNull(fldIndex) ? null : values[fldIndex]);
                        }
                        catch(Exception)
                        {
                            continue;
                        }
                    }
                    result.Add(instance);
                }
            }
            finally
            {
                conn.Close();
            }
            return result;
        }

        private static int GetFieldIndex(string name, IDataReader reader)
        {
            try
            {
                return reader.GetOrdinal(name);
            }
            catch (Exception)
            {
                return -1;
            }
        }

        public static IEnumerable<T> FromSQLRaw1<T>(this IDbConnection conn, string sql) where T : class, new()
        {
            List<T> result = new List<T>();
            conn.Open();
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                using var dr = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection);
                Dictionary<PropertyInfo, int> fieldMap = null;
                while (dr.Read())
                {
                    var instance = new T();
                    var values = new object[dr.FieldCount];

                    if (fieldMap == null)
                        fieldMap = typeof(T).GetProperties().Select(a => new
                        { PropInfo = a, Index = GetFieldIndex(a.Name, dr) }).Where(a => a.Index != -1).OrderBy(a => a.Index).ToDictionary(a => a.PropInfo, v => v.Index);

                    dr.GetValues(values);
                    foreach (var prop in fieldMap)
                    {
                        prop.Key.SetValue(instance, values[prop.Value] == DBNull.Value ? null : values[prop.Value]);
                    }
                    result.Add(instance);
                }
            }
            finally
            {
                conn.Close();
            }
            return result;
        }

        private static Action<TType, object> CreateTo<TType>(string property)
        {
            var type = typeof(TType);
            var pi = type.GetProperty(property);
            if (pi == null)
                throw new ArgumentException($"property {property} not exists");
            var method = new DynamicMethod($"{type.Name}_{property}_Setter", typeof(void), new[] { type, typeof(object) }, typeof(RowMappingHandler));
            var ilgen = method.GetILGenerator();
            ilgen.Emit(OpCodes.Ldarg_0);
            ilgen.Emit(OpCodes.Ldarg_1);
            if (pi.PropertyType.IsValueType)
            {
                ilgen.Emit(OpCodes.Unbox_Any, pi.PropertyType);
                ilgen.Emit(OpCodes.Call, pi.GetSetMethod());
            }
            else
            {
                ilgen.Emit(OpCodes.Castclass, pi.PropertyType);
                ilgen.Emit(OpCodes.Callvirt, pi.GetSetMethod());
            }

            ilgen.Emit(OpCodes.Ret);
            return method.CreateDelegate(typeof(Action<TType, object>)) as Action<TType, object>;
        }

        public static IEnumerable<T> FromSQLRaw2<T>(this IDbConnection conn, string sql) where T : class, new()
        {
            List<T> result = new List<T>();
            conn.Open();
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                using var dr = cmd.ExecuteReader(CommandBehavior.SequentialAccess | CommandBehavior.CloseConnection);
                Dictionary<string, Delegate> mappedType = new Dictionary<string, Delegate>();
                Dictionary<string, int> fieldMap = null;
                while (dr.Read())
                {
                    var instance = new T();
                    if (fieldMap == null)
                        fieldMap = typeof(T).GetProperties().Select(a => new
                        { a.Name, Index = GetFieldIndex(a.Name, dr) }).Where(a => a.Index != -1).OrderBy(a => a.Index).ToDictionary(a => a.Name, v => v.Index);

                    var values = new object[dr.FieldCount];
                    dr.GetValues(values);
                    foreach (var pi in fieldMap)
                    {
                        if (!mappedType.ContainsKey(pi.Key))
                            mappedType[pi.Key] = CreateTo<T>(pi.Key);
                        ((Action<T, object>)mappedType[pi.Key])(instance, values[fieldMap[pi.Key]] == DBNull.Value ? null : values[fieldMap[pi.Key]]);
                    }
                    result.Add(instance);
                }
            }
            finally
            {
                conn.Close();
            }
            return result;
        }
    }

    class Program
    {
        static void GenerateCustomersV1()
        {
            var context = new Myefc_DB2Context();
            context.Database.EnsureCreated();
            const string characters = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            var random = new Random();
            for (int i = 0; i < 100000; i++)
            {
                context.Customers.Add(new Customers()
                {
                    Name = new string(Enumerable.Repeat(characters, 10).Select(a => a[random.Next(a.Length)]).ToArray()),
                    Credit_Level = random.Next(5)
                });
            }
            context.SaveChanges();
        }

        static void QueryWithDapper()
        {
            var conn = new SqlConnection(@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=MyEFC_DB2;Integrated Security=True;Connect Timeout=30;Encrypt=False");
            conn.Open();
            try
            {
                var data = conn.Query<Customers>(@"SELECT * FROM Customers");
                Console.WriteLine(data.Count());
            }
            finally
            {
                conn.Close();
            }
        }

        static void QueryWithEF()
        {
            var context = new Myefc_DB2Context();
            var data = context.Customers.ToList();
            Console.WriteLine(data.Count());
        }

        static void QueryWithEF_NoTrack()
        {
            var context = new Myefc_DB2Context();
            context.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
            var data = context.Customers.AsNoTracking().ToList();
            Console.WriteLine(data.Count());
        }

        static void QueryWithEF_SQL()
        {
            var context = new Myefc_DB2Context();
            var data = context.Customers.FromSqlRaw(@"SELECT * FROM Customers").AsNoTracking().ToList();
            Console.WriteLine(data.Count());
        }

        static void QueryWithEF_SQL2()
        {
            var context = new Myefc_DB2Context();
            var conn = new SqlConnection(@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=MyEFC_DB2;Integrated Security=True;Connect Timeout=30;Encrypt=False");
            var data = conn.FromSQLRaw5<Customers>(@"SELECT * FROM Customers");
            Console.WriteLine(data.Count());
        }

        static void Benchmark(Action f, string label)
        {
            var sw = new Stopwatch();
            sw.Start();
            f();
            sw.Stop();
            Console.WriteLine($"{label} elapsed {sw.ElapsedMilliseconds} ms");
        }

        static void TestIL(Customers c, int value)
        {
            c.Id = (int)value;
        }

        static void Main(string[] args)
        {
            Benchmark(QueryWithDapper, "Dapper");
            //Benchmark(QueryWithEF, "EF Core");
            //Benchmark(QueryWithEF_NoTrack, "EF Core(No Tracking)");
            Benchmark(QueryWithEF_SQL, "EF Core(SQL)");
            Benchmark(QueryWithEF_SQL2, "SQL2");

            Benchmark(QueryWithDapper, "Dapper");
            //Benchmark(QueryWithEF, "EF Core");
            //Benchmark(QueryWithEF_NoTrack, "EF Core(No Tracking)");
            Benchmark(QueryWithEF_SQL, "EF Core(SQL)");
            Benchmark(QueryWithEF_SQL2, "SQL2");

            Benchmark(QueryWithDapper, "Dapper");
            //Benchmark(QueryWithEF, "EF Core");
            //Benchmark(QueryWithEF_NoTrack, "EF Core(No Tracking)");
            Benchmark(QueryWithEF_SQL, "EF Core(SQL)");
            Benchmark(QueryWithEF_SQL2, "SQL2");


            Console.ReadLine();
        }
    }
}
