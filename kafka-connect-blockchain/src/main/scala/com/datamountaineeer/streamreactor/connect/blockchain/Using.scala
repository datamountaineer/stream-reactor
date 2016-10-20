/**
  * Copyright 2016 Datamountaineer.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  **/

package com.datamountaineeer.streamreactor.connect.blockchain

trait Using {
  def using[AC<:AutoCloseable, R](autoCloseable: AC)(thunk: AC => R): R = {
    try {
      thunk(autoCloseable)
    }
    finally {
      if (autoCloseable != null) autoCloseable.close()
    }
  }

  /*
  def using[T <: {def close() : Unit}, R](t: T)(thunk: T => R): R = {
    try {
      thunk(t)
    } finally {
      if (t != null) t.close()
    }
  }*/
}
