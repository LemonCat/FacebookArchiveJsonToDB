using Newtonsoft.Json;
using System;
using System.Data.SqlClient;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;

namespace JsonToDB
{

    class Program
    {
        static void Main(string[] args)
        {
            Console.OutputEncoding = System.Text.Encoding.UTF8;

            /*PREPARE CONNEXIONS AND DATATABLES*/
            SqlConnection sqlcon = new SqlConnection(@"Server=localhost\SQLEXPRESS;Database=fb_stats;Trusted_Connection=True;");

            DataTable dtMessage = new DataTable();
            dtMessage.Columns.Add(new DataColumn("sender_name", typeof(string)));
            dtMessage.Columns.Add(new DataColumn("timestamp_ms", typeof(Int64)));
            dtMessage.Columns.Add(new DataColumn("content", typeof(string)));
            dtMessage.Columns.Add(new DataColumn("type", typeof(string)));
            dtMessage.Columns.Add(new DataColumn("share", typeof(string)));

            DataTable dtPhoto = new DataTable();
            dtPhoto.Columns.Add(new DataColumn("uri", typeof(string)));
            dtPhoto.Columns.Add(new DataColumn("creation_timestamp", typeof(Int64)));
            dtPhoto.Columns.Add(new DataColumn("messageID", typeof(Int64)));

            DataTable dtReaction = new DataTable();
            dtReaction.Columns.Add(new DataColumn("reaction", typeof(string)));
            dtReaction.Columns.Add(new DataColumn("actor", typeof(string)));
            dtReaction.Columns.Add(new DataColumn("messageID", typeof(Int64)));


            /*Parse JSON*/
            //PUT HERE THE FOLDER WHERE YOUR JSON FILES ARE LOCATED
            string baseFolder = @"F:\facebook\";
            
            string[] fileEntries = Directory.GetFiles(baseFolder);
            foreach (string fileName in fileEntries)
            {
                if (fileName.Contains("message_"))
                {
                    string myJSON = System.IO.File.ReadAllText(fileName, Encoding.UTF8);


                    var myRootObj = JsonConvert.DeserializeObject<RootObject>(myJSON);

                    foreach (Message one_msg in myRootObj.messages)
                    {
                        DataRow dr = dtMessage.NewRow();
                        
                        
                        dr["sender_name"] = DecodeFromUtf8(one_msg.sender_name);
                        dr["timestamp_ms"] = one_msg.timestamp_ms;
                        dr["content"] = DecodeFromUtf8(one_msg.content);
                        dr["type"] = DecodeFromUtf8(one_msg.type);

                        if (one_msg.share != null)
                        {
                            dr["share"] = DecodeFromUtf8(one_msg.share.link);
                        }
                        dtMessage.Rows.Add(dr);

                        //insert msg and get id
                        long curID = MessageInsert(sqlcon, dtMessage);
                        
                        if (one_msg.reactions != null)
                        {
                            foreach (Reaction one_reaction in one_msg.reactions)
                            {
                                DataRow drR = dtReaction.NewRow();
                                drR["reaction"] = DecodeFromUtf8(one_reaction.reaction);
                                drR["actor"] = DecodeFromUtf8(one_reaction.actor);
                                drR["messageID"] = curID;
                                dtReaction.Rows.Add(drR);
                            }
                        }

                        if (one_msg.photos != null)
                        {
                            foreach (Photo one_photo in one_msg.photos)
                            {
                                DataRow drP = dtPhoto.NewRow();
                                drP["uri"] = DecodeFromUtf8(one_photo.uri);
                                drP["creation_timestamp"] = one_photo.creation_timestamp;
                                drP["messageID"] = curID;
                                dtPhoto.Rows.Add(drP);
                            }

                        }

                        ReactionAndPhotoInsert(sqlcon, dtPhoto, dtReaction);

                        //Reinit datatable
                        dtMessage.Clear();
                        dtPhoto.Clear();
                        dtReaction.Clear();
                    }
                }
            }
            sqlcon.Close();

        }

        static long MessageInsert(SqlConnection sqlcon, DataTable dtMessage)
        {
            if (sqlcon.State == ConnectionState.Closed)
                sqlcon.Open();

            //Decoder utf8Decoder = Encoding.UTF8.GetDecoder();
            SqlCommand cmdMsg = new SqlCommand("INSERT INTO [dbo].[Message] ([sender_name] ,[timestamp_ms] ,[content] ,[type] ,[link])"
                                             + " OUTPUT INSERTED.ID"
                                             + " VALUES(@sender_name, @timestamp_ms, @content, @type, @link)", sqlcon);



            foreach (DataRow curRow in dtMessage.Rows)
            {
                cmdMsg.Parameters.AddWithValue("sender_name", curRow["sender_name"]);
                cmdMsg.Parameters.AddWithValue("timestamp_ms", curRow["timestamp_ms"]);
                cmdMsg.Parameters.AddWithValue("content", curRow["content"]);
                cmdMsg.Parameters.AddWithValue("type", curRow["type"]);
                cmdMsg.Parameters.AddWithValue("link", curRow["share"]);
            }
            
            //Insert Msg
            SqlDataReader reader = cmdMsg.ExecuteReader();

            long curID=0;
            if (reader.Read())
            {                 
                curID = reader.GetInt64("ID");
            }
            reader.Close();

            return curID;
            


        }
        static void ReactionAndPhotoInsert(SqlConnection sqlcon, DataTable dtPhoto, DataTable dtReaction)
        {
            if (sqlcon.State == ConnectionState.Closed)
                sqlcon.Open();


            
            //Insert photo
            foreach (DataRow curPhotoRow in dtPhoto.Rows)
            {
                SqlCommand cmdPhoto = new SqlCommand("INSERT INTO [dbo].[Photo] ([uri] ,[creation_timestamp] ,[messageID])"
                                       + " VALUES(@uri, @creation_timestamp, @messageID)", sqlcon);
                cmdPhoto.Parameters.AddWithValue("uri", curPhotoRow["uri"]);
                cmdPhoto.Parameters.AddWithValue("creation_timestamp", curPhotoRow["creation_timestamp"]);
                cmdPhoto.Parameters.AddWithValue("messageID", curPhotoRow["messageID"]);
                cmdPhoto.ExecuteNonQuery();
                //cmdPhoto.Dispose();
            }

            foreach (DataRow curReactionRow in dtReaction.Rows)
            {
                SqlCommand cmdReaction = new SqlCommand("INSERT INTO [dbo].[Reaction]([reaction],[actor],[messageID])"
                                               + " VALUES (@reaction,@actor, @messageID)", sqlcon);

                cmdReaction.Parameters.AddWithValue("reaction", curReactionRow["reaction"]);
                cmdReaction.Parameters.AddWithValue("actor", curReactionRow["actor"]);
                cmdReaction.Parameters.AddWithValue("messageID", curReactionRow["messageID"]);
                cmdReaction.ExecuteNonQuery();
                //cmdReaction.Dispose();
            }
        }

            public static string DecodeFromUtf8(string utf8String)
        {
            // copy the string as UTF-8 bytes.
            if (utf8String != null)
            {
                byte[] utf8Bytes = new byte[utf8String.Length];
                for (int i = 0; i < utf8String.Length; ++i)
                {
                    //Debug.Assert( 0 <= utf8String[i] && utf8String[i] <= 255, "the char must be in byte's range");
                    utf8Bytes[i] = (byte)utf8String[i];
                }

                return Encoding.UTF8.GetString(utf8Bytes, 0, utf8Bytes.Length);
            }else
            {
                return "";
            }
            
        }
    }
}

