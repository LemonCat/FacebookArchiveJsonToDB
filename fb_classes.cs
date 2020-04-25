using System;
using System.Collections.Generic;
using System.Text;

namespace JsonToDB
{
    public class Participant
    {
        public string name { get; set; }
    }

    public class Reaction
    {
        public string reaction { get; set; }
        public string actor { get; set; }
    }

    public class Message
    {
        public string sender_name { get; set; }
        public Int64 timestamp_ms { get; set; }
        public string content { get; set; }
        public string type { get; set; }
        public List<Reaction> reactions { get; set; }
        public List<Photo> photos { get; set; }
        public Share share { get; set; }
    }

    public class Share
    {
        public string link { get; set; }
    }

    public class Photo
    {
        public string uri { get; set; }
        public int creation_timestamp { get; set; }
    }

    public class RootObject
    {
        public List<Participant> participants { get; set; }
        public List<Message> messages { get; set; }
        public string title { get; set; }
        public bool is_still_participant { get; set; }
        public string thread_type { get; set; }
        public string thread_path { get; set; }
    }
}
