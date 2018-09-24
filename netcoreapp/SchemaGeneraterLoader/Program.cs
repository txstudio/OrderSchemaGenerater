using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace SchemaGeneraterLoader
{
    class Program
    {
        const string ConnectionString = "Server=192.168.0.80;Database=OrderSchemaGenerater;User Id=sa;Password=Pa$$w0rd;";

        static LoaderOptions _option = new LoaderOptions();
        static Random _random = new Random();
        static bool _exit = false;

        static int _totalCount = 0;
        static int _nextDateCount = 0;
        static int _nextRangeCount = 150;
        static int _maxCount = 500;

        static DateTime _currentDate = DateTime.Now;

        static void Main(string[] args)
        {
            args = new string[] { "-t", "250" }; 
            SetArgs(_option, args);

            List<Task> _tasks;

            _tasks = new List<Task>();

            for (int i = 0; i < _option.Task; i++)
                _tasks.Add(new Task(OrderGenerater));

            for (int i = 0; i < _option.Task; i++)
                _tasks[i].Start();

            Stopwatch _stopwatch = new Stopwatch();

            while (_exit == false)
            {
                _exit = true;

                for (int i = 0; i < _option.Task; i++)
                {
                    if (_tasks[i].Status == TaskStatus.Running)
                        _stopwatch.Start();

                    if (_tasks[i].Status == TaskStatus.Running
                        || _tasks[i].Status == TaskStatus.WaitingToRun)
                    {
                        Thread.Sleep(100);
                        _exit = false;
                        continue;
                    }
                }

                if (_exit == false)
                    continue;

                _exit = true;
                _stopwatch.Stop();

                Thread.Sleep(1000);
            }

            Console.WriteLine();
            Console.WriteLine("測試結束，共花費 {0}", _stopwatch.Elapsed);
            Console.WriteLine();
            Console.WriteLine("press any key to exit");
            Console.ReadKey();
        }

        private static void OrderGenerater()
        {
            Stopwatch _stopwatch = new Stopwatch();
            
            while (true)
            {
                _stopwatch.Reset();
                _stopwatch.Start();

                using (SqlConnection _conn = new SqlConnection())
                {
                    _conn.ConnectionString = ConnectionString;

                    SqlCommand _cmd;

                    _cmd = new SqlCommand();
                    _cmd.Connection = _conn;

                    _cmd.CommandType = CommandType.StoredProcedure;
                    _cmd.CommandText = "[Orders].[AddOrder]";

                    _cmd.Parameters.Add("@CurrentDate", SqlDbType.Date);
                    _cmd.Parameters.Add("@Success", SqlDbType.Bit);

                    _cmd.Parameters["@CurrentDate"].Value = GetCurrentDate();
                    _cmd.Parameters["@Success"].Value = DBNull.Value;

                    _cmd.Parameters["@CurrentDate"].Direction = ParameterDirection.Input;
                    _cmd.Parameters["@Success"].Direction = ParameterDirection.Output;

                    _conn.Open();
                    _cmd.ExecuteNonQuery(); 
                    _conn.Close();
                }

                _stopwatch.Stop();

                Console.WriteLine("count: {0}\tdatetime: {1:yyyy-MM-dd}\tmilliseconds: {2}"
                                , _totalCount
                                , _currentDate
                                , _stopwatch.ElapsedMilliseconds);

                AddCount();

                //總執行次數超過指定數值結束測試
                if (Exit() == true)
                    break;
            }
        }

        static void SetArgs(LoaderOptions option, string[] args)
        {
            var _arg = string.Empty;
            var _index = 0;

            for (int i = 0; i < args.Length; i++)
            {
                _arg = args[i];
                _index = i + 1;

                if (_index <= args.Length)
                {
                    switch (_arg)
                    {
                        case "-t":
                            option.TaskNumber = args[_index];
                            break;
                        default:
                            break;
                    }
                }
            }

            Console.WriteLine("-------------------------");
            Console.WriteLine("Task 資訊");
            Console.WriteLine("-------------------------");
            Console.WriteLine("起始時間:{0}\t總數:{1}"
                            , option.StartTime
                            , option.Task);

        }

        static void AddCount()
        {
            _totalCount = _totalCount + 1;
            _nextDateCount = _nextDateCount + 1;

            if (_nextDateCount > _nextRangeCount)
            {
                _currentDate = _currentDate.AddDays(1);
                _nextDateCount = 0;
            }
        }

        static DateTime GetCurrentDate()
        {
            return _currentDate;
        }

        static bool Exit()
        {
            if (_totalCount > _maxCount)
                return true;

            return false;
        }

    }


}
