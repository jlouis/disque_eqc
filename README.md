# A QuickCheck model for sequential disque

This little piece of Erlang code models a sequential version of disque, for the very basic set
of commands in the system. Using QuickCheck one can then generate random test cases for
sequential disque in order to find errors in the code base.

Maybe we will model parallel access and cluster code and check what distributed properties
the system has.

# Copyright notice:

   Copyright 2015 Jesper Louis Andersen

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
