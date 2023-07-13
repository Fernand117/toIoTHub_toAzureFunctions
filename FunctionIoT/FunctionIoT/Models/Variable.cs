using System.Collections.Generic;

namespace FunctionIoT.Models
{
    public class Variable
    {
        public static bool isListAvailable;
        public static List<Variable> variableList = new List<Variable>();

        public int idLine;
        public string fieldName;
        public string tagName;
        public int type;
    }
}
