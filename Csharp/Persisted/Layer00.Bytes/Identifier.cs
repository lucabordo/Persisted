using System;
using System.Collections.Generic;
using System.IO;

namespace Persisted
{
    /// <summary>
    /// An string that obeys some conventions and is used to identify containers within a storage.
    /// <para>
    /// The convention is that the only allowed characters are:
    ///   (1) Alphabetical characters:
    ///       lower-case ones, 
    ///       and upper-case ones, which are standardized to lower case.
    ///   (2) Numerical characters;
    ///   (3) The underscore character '_'
    ///   (4) The slash '/' character. By convention this indicates sub-directories,
    ///       even though some storages might use a different separator, or indeed have 
    ///       no concept of directory at all.
    ///       Two consecutive occurrences of Slash are not allowed.
    /// </para>
    /// <para>
    ///   The dot character '.' is now allowed but is used internally with a special meaning:
    ///   a specific storage might allocate several files or storage units for a given container,
    ///   in which case suffixes using the dot wil be used.
    /// </para>
    /// </summary>
    public struct Identifier
    {
        #region Private

        private string _text;

        private Identifier(string nonStandardizedName)
        {
            _text = Identifier.Standardize(nonStandardizedName);
        }

        #endregion

        #region Methods 

        public static implicit operator string(Identifier id)
        {
            return id._text;
        }

        public static implicit operator Identifier(string nonStandardizedId)
        {
            return new Identifier(nonStandardizedId);
        }

        #endregion

        #region Allowed characters

        /// <summary>
        /// True if a character is allowed
        /// </summary>
        public static bool IsAllowed(char c)
        {
            return
                'a' <= c && c <= 'z' ||
                'A' <= c && c <= 'Z' ||
                '0' <= c && c <= '9' ||
                c == '_' || c == '/';
        }

        /// <summary>
        /// Gets the set of allowed characters 
        /// </summary>
        public static IEnumerable<char> GetAllowedCharacters()
        {
            for (char c = 'a'; c <= 'z'; c++)
                yield return c;

            for (char c = 'A'; c <= 'Z'; c++)
                yield return c;

            for (char c = '0'; c <= '9'; c++)
                yield return c;

            yield return '_';
            yield return '/';
        }

        #endregion

        #region Standardization

        /// <summary>
        /// Check that an ID is correct, and standardize it by replacing any 
        /// upper-case character to lower case, and optionally the directory separator by one
        /// specific to the considered storage.
        /// </summary>
        /// <param name="id">A document identifier</param>
        /// <param name="dirSeparator">
        /// A separator that is substituted to the standard directory separator
        /// </param>
        public static string Standardize(string id, char dirSeparator)
        {
            var chars = new char[id.Length];

            for (int i = 0; i < id.Length; ++i)
            {
                char c = id[i];

                if ('a' <= c && c <= 'z')
                {
                    // just keep c
                }
                else if ('A' <= c && c <= 'Z')
                {
                    c = (char)(c - ('A' - 'a'));
                }
                else if ('0' <= c && c <= '9')
                {
                    // just keep c
                }
                else if (c == '_')
                {
                    // just keep c
                }
                else if (c == '/')
                {
                    if (i > 0 && id[i - 1] == '/')
                        throw new ArgumentException("Consecutive Directory separators not allowed");
                    c = dirSeparator;
                }
                else
                {
                    throw new ArgumentException("Character not allowed " + ((int)c).ToString());
                }

                chars[i] = c;
            }

            return new string(chars, 0, id.Length);
        }

        /// <summary>
        /// Check that an ID is correct, and standardize it by replacing any 
        /// upper-case character to lower case, and optionally the directory separator by one
        /// specific to the considered storage.
        /// </summary>
        /// <param name="id">A document identifier</param>
        /// <param name="localizeDirectorySeparator">
        /// True if we replace the standard directory separator by the one specific to the current OS
        /// </param>
        public static string Standardize(string id, bool localizeDirectorySeparator = true)
        {
            return Standardize(id, localizeDirectorySeparator ? Path.DirectorySeparatorChar : '/');
        }

        #endregion
    }
}
