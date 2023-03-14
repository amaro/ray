// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "ray/object_manager/plasma/protocol.h"

#include <fstream>
#include <iostream>
#include <utility>

#include "flatbuffers/flatbuffers.h"
#include "ray/object_manager/plasma/common.h"
#include "ray/object_manager/plasma/connection.h"
#include "ray/object_manager/plasma/plasma_generated.h"

namespace fb = plasma::flatbuf;

namespace plasma {

using fb::MessageType;
using fb::PlasmaError;
using fb::PlasmaObjectSpec;

using flatbuffers::uoffset_t;

namespace internal {

static uint8_t non_null_filler;

}  // namespace internal

class Tracer {
  using object_id_vec = flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>;
  using plasma_obj_vec = flatbuffers::Vector<const PlasmaObjectSpec *>;

 public:
  Tracer() {}

  void log(const std::string &message) {
    std::ofstream file("/tmp/raytracer", std::ios::app);
    file << message;
    file.close();
  }

  // traces messages from client -> plasma, and plasma -> client
  void TracePlasmaMessage(MessageType message_type,
                          uoffset_t size,
                          const uint8_t *buffer) {
    std::ostringstream stream;

    stream << fb::EnumNamesMessageType()[static_cast<int32_t>(message_type)] << "\n";

    switch (message_type) {
    case MessageType::PlasmaCreateRequest: {
      auto m = flatbuffers::GetRoot<fb::PlasmaCreateRequest>(buffer);
      parse_object_id(stream, m->object_id()->str());
      stream << "  owner_raylet_id: " << NodeID::FromBinary(m->owner_raylet_id()->str())
             << "\n";
      stream << "  owner_ip_address: " << m->owner_ip_address()->str() << "\n";
      stream << "  owner_port: " << m->owner_port() << "\n";
      stream << "  owner_worker_id: " << WorkerID::FromBinary(m->owner_worker_id()->str())
             << "\n";
      stream << "  data_size: " << m->data_size() << "\n";
      stream << "  metadata_size: " << m->metadata_size() << "\n";
      stream << "  source: "
             << fb::EnumNamesObjectSource()[static_cast<int32_t>(m->source())] << "\n";
      stream << "  device_num: " << m->device_num() << "\n";
      stream << "  try_immediately: " << m->try_immediately() << "\n";
    } break;
    case MessageType::PlasmaCreateRetryRequest: {
      auto m = flatbuffers::GetRoot<fb::PlasmaCreateRetryRequest>(buffer);
      parse_object_id(stream, m->object_id()->str());
      stream << "  request_id: " << m->request_id() << "\n";
    } break;
    case MessageType::PlasmaCreateReply: {
      auto m = flatbuffers::GetRoot<fb::PlasmaCreateReply>(buffer);
      parse_object_id(stream, m->object_id()->str());
      stream << "  retry_with_request_id: " << m->retry_with_request_id() << "\n";
      if (m->retry_with_request_id() == 0) {
        parse_plasma_object(stream, m->plasma_object());
        parse_plasma_error(stream, m->error());
        stream << "  store_fd: " << m->store_fd() << "\n";
        stream << "  unique_fd_id: " << m->unique_fd_id() << "\n";
        stream << "  mmap_size: " << m->mmap_size() << "\n";
      }
      // TODO: m->ipc_handle
    } break;
    case MessageType::PlasmaAbortRequest: {
      auto m = flatbuffers::GetRoot<fb::PlasmaAbortRequest>(buffer);
      parse_object_id(stream, m->object_id()->str());
    } break;
    case MessageType::PlasmaAbortReply: {
      auto m = flatbuffers::GetRoot<fb::PlasmaAbortReply>(buffer);
      parse_object_id(stream, m->object_id()->str());
    } break;
    case MessageType::PlasmaSealRequest: {
      auto m = flatbuffers::GetRoot<fb::PlasmaSealRequest>(buffer);
      parse_object_id(stream, m->object_id()->str());
    } break;
    case MessageType::PlasmaSealReply: {
      auto m = flatbuffers::GetRoot<fb::PlasmaSealReply>(buffer);
      parse_object_id(stream, m->object_id()->str());
      parse_plasma_error(stream, m->error());
    } break;
    case MessageType::PlasmaGetRequest: {
      auto m = flatbuffers::GetRoot<fb::PlasmaGetRequest>(buffer);
      parse_object_ids(stream, m->object_ids());
      stream << "  timeout_ms: " << m->timeout_ms() << "\n";
      stream << "  is_from_worker: " << m->is_from_worker() << "\n";
    } break;
    case MessageType::PlasmaGetReply: {
      auto m = flatbuffers::GetRoot<fb::PlasmaGetReply>(buffer);
      parse_object_ids(stream, m->object_ids());
      parse_plasma_objects(stream, m->plasma_objects());
      parse_int_vec(stream, "store_fds", m->store_fds());
      parse_int_vec(stream, "unique_fd_ids", m->unique_fd_ids());
      parse_int_vec(stream, "mmap_sizes", m->mmap_sizes());
      // TODO: m->handles
    } break;
    case MessageType::PlasmaReleaseRequest: {
      auto m = flatbuffers::GetRoot<fb::PlasmaReleaseRequest>(buffer);
      parse_object_id(stream, m->object_id()->str());
    } break;
    case MessageType::PlasmaReleaseReply: {
      auto m = flatbuffers::GetRoot<fb::PlasmaReleaseReply>(buffer);
      parse_object_id(stream, m->object_id()->str());
      parse_plasma_error(stream, m->error());
    } break;
    case MessageType::PlasmaDeleteRequest: {
      auto m = flatbuffers::GetRoot<fb::PlasmaDeleteRequest>(buffer);
      stream << "  count: " << m->count() << "\n";
      parse_object_ids(stream, m->object_ids());
    } break;
    case MessageType::PlasmaDeleteReply: {
      auto m = flatbuffers::GetRoot<fb::PlasmaDeleteReply>(buffer);
      stream << "  count: " << m->count() << "\n";
      parse_object_ids(stream, m->object_ids());
      parse_plasma_errors(stream, m->errors());
    } break;
    case MessageType::PlasmaContainsRequest: {
      auto m = flatbuffers::GetRoot<fb::PlasmaContainsRequest>(buffer);
      parse_object_id(stream, m->object_id()->str());
    } break;
    case MessageType::PlasmaContainsReply: {
      auto m = flatbuffers::GetRoot<fb::PlasmaContainsReply>(buffer);
      parse_object_id(stream, m->object_id()->str());
      stream << "  has_object: " << m->has_object() << "\n";
    } break;
    case MessageType::PlasmaConnectReply: {
      auto m = flatbuffers::GetRoot<fb::PlasmaConnectReply>(buffer);
      stream << "  memory_capacity: " << m->memory_capacity() << "\n";
    } break;
    case MessageType::PlasmaEvictRequest: {
      auto m = flatbuffers::GetRoot<fb::PlasmaEvictRequest>(buffer);
      stream << "  num_bytes: " << m->num_bytes() << "\n";
    } break;
    case MessageType::PlasmaEvictReply: {
      auto m = flatbuffers::GetRoot<fb::PlasmaEvictReply>(buffer);
      stream << "  num_bytes: " << m->num_bytes() << "\n";
    } break;
    default:
      stream << "  NOT HANDLED\n";
      break;
    }

    log(stream.str());
  }

 private:
  void parse_object_id(std::ostringstream &stream, const std::string &object_id) {
    stream << "  object_id: " << ray::ObjectID::FromBinary(object_id) << "\n";
  }

  void parse_object_ids(std::ostringstream &stream, const object_id_vec *object_ids) {
    for (uoffset_t i = 0; i < object_ids->size(); ++i) {
      auto object_id = object_ids->Get(i)->str();
      parse_object_id(stream, object_id);
    }
  }

  void parse_plasma_error(std::ostringstream &stream,
                          plasma::flatbuf::PlasmaError error) {
    stream << "  error: " << fb::EnumNamesPlasmaError()[static_cast<int32_t>(error)]
           << "\n";
  }

  void parse_plasma_errors(std::ostringstream &stream,
                           const flatbuffers::Vector<int32_t> *errors) {
    for (auto i = 0u; i < errors->size(); ++i) {
      parse_plasma_error(stream, static_cast<PlasmaError>(errors->Get(i)));
    }
  }

  void parse_plasma_object(std::ostringstream &stream,
                           const plasma::flatbuf::PlasmaObjectSpec *plasma_object) {
    stream << "  plasma_object:\n";
    stream << "    segment_index: " << plasma_object->segment_index() << "\n";
    stream << "    unique_fd_id: " << plasma_object->unique_fd_id() << "\n";
    stream << "    data_offset: " << plasma_object->data_offset() << "\n";
    stream << "    data_size: " << plasma_object->data_size() << "\n";
    stream << "    metadata_offset: " << plasma_object->metadata_offset() << "\n";
    stream << "    metadata_size: " << plasma_object->metadata_size() << "\n";
    stream << "    device_num: " << plasma_object->device_num() << "\n";
  }

  void parse_plasma_objects(std::ostringstream &stream,
                            const plasma_obj_vec *plasma_objs) {
    for (uoffset_t i = 0; i < plasma_objs->size(); ++i) {
      auto plasma_obj = plasma_objs->Get(i);
      parse_plasma_object(stream, plasma_obj);
    }
  }

  template <typename T>
  void parse_int_vec(std::ostringstream &stream,
                     const std::string &tag,
                     const flatbuffers::Vector<T> *vec) {
    stream << "  " << tag << ": ";
    for (auto i = 0u; i < vec->size(); ++i) {
      stream << vec->Get(i) << " ";
    }
    stream << "\n";
  }
};

static Tracer raytracer;

/// \brief Returns maybe_null if not null or a non-null pointer to an arbitrary memory
/// that shouldn't be dereferenced.
///
/// Memset/Memcpy are undefined when a nullptr is passed as an argument use this utility
/// method to wrap locations where this could happen.
///
/// Note: Flatbuffers has UBSan warnings if a zero length vector is passed.
/// https://github.com/google/flatbuffers/pull/5355 is trying to resolve
/// them.
template <typename T>
inline T *MakeNonNull(T *maybe_null) {
  if (RAY_PREDICT_TRUE(maybe_null != nullptr)) {
    return maybe_null;
  }
  return reinterpret_cast<T *>(&internal::non_null_filler);
}

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
ToFlatbuffer(flatbuffers::FlatBufferBuilder *fbb,
             const ObjectID *object_ids,
             int64_t num_objects) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (int64_t i = 0; i < num_objects; i++) {
    results.push_back(fbb->CreateString(object_ids[i].Binary()));
  }
  return fbb->CreateVector(MakeNonNull(results.data()), results.size());
}

flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<flatbuffers::String>>>
ToFlatbuffer(flatbuffers::FlatBufferBuilder *fbb,
             const std::vector<std::string> &strings) {
  std::vector<flatbuffers::Offset<flatbuffers::String>> results;
  for (size_t i = 0; i < strings.size(); i++) {
    results.push_back(fbb->CreateString(strings[i]));
  }

  return fbb->CreateVector(MakeNonNull(results.data()), results.size());
}

flatbuffers::Offset<flatbuffers::Vector<int64_t>> ToFlatbuffer(
    flatbuffers::FlatBufferBuilder *fbb, const std::vector<int64_t> &data) {
  return fbb->CreateVector(MakeNonNull(data.data()), data.size());
}

Status PlasmaReceive(const std::shared_ptr<StoreConn> &store_conn,
                     MessageType message_type,
                     std::vector<uint8_t> *buffer) {
  if (!store_conn) {
    return Status::IOError("Connection is closed.");
  }
  return store_conn->ReadMessage(static_cast<int64_t>(message_type), buffer);
}

// Helper function to create a vector of elements from Data (Request/Reply struct).
// The Getter function is used to extract one element from Data.
template <typename T, typename Data, typename Getter>
void ToVector(const Data &request, std::vector<T> *out, const Getter &getter) {
  int count = request.count();
  out->clear();
  out->reserve(count);
  for (int i = 0; i < count; ++i) {
    out->push_back(getter(request, i));
  }
}

template <typename T, typename FlatbufferVectorPointer, typename Converter>
void ConvertToVector(const FlatbufferVectorPointer fbvector,
                     std::vector<T> *out,
                     const Converter &converter) {
  out->clear();
  out->reserve(fbvector->size());
  for (size_t i = 0; i < fbvector->size(); ++i) {
    out->push_back(converter(*fbvector->Get(i)));
  }
}

template <typename Message>
Status PlasmaSend(const std::shared_ptr<StoreConn> &store_conn,
                  MessageType message_type,
                  flatbuffers::FlatBufferBuilder *fbb,
                  const Message &message) {
  if (!store_conn) {
    return Status::IOError("Connection is closed.");
  }
  fbb->Finish(message);
  raytracer.TracePlasmaMessage(message_type, fbb->GetSize(), fbb->GetBufferPointer());
  return store_conn->WriteMessage(
      static_cast<int64_t>(message_type), fbb->GetSize(), fbb->GetBufferPointer());
}

template <typename Message>
Status PlasmaSend(const std::shared_ptr<Client> &client,
                  MessageType message_type,
                  flatbuffers::FlatBufferBuilder *fbb,
                  const Message &message) {
  if (!client) {
    return Status::IOError("Connection is closed.");
  }
  fbb->Finish(message);
  raytracer.TracePlasmaMessage(message_type, fbb->GetSize(), fbb->GetBufferPointer());
  return client->WriteMessage(
      static_cast<int64_t>(message_type), fbb->GetSize(), fbb->GetBufferPointer());
}

Status PlasmaErrorStatus(fb::PlasmaError plasma_error) {
  switch (plasma_error) {
  case fb::PlasmaError::OK:
    return Status::OK();
  case fb::PlasmaError::ObjectExists:
    return Status::ObjectExists("object already exists in the plasma store");
  case fb::PlasmaError::ObjectNonexistent:
    return Status::ObjectNotFound("object does not exist in the plasma store");
  case fb::PlasmaError::OutOfMemory:
    return Status::ObjectStoreFull("object does not fit in the plasma store");
  case fb::PlasmaError::OutOfDisk:
    return Status::OutOfDisk("Local disk is full");
  case fb::PlasmaError::UnexpectedError:
    return Status::UnknownError(
        "an unexpected error occurred, likely due to a bug in the system or caller");
  default:
    RAY_LOG(FATAL) << "unknown plasma error code " << static_cast<int>(plasma_error);
  }
  return Status::OK();
}

// Get debug string messages.

Status SendGetDebugStringRequest(const std::shared_ptr<StoreConn> &store_conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaGetDebugStringRequest(fbb);
  return PlasmaSend(store_conn, MessageType::PlasmaGetDebugStringRequest, &fbb, message);
}

Status SendGetDebugStringReply(const std::shared_ptr<Client> &client,
                               const std::string &debug_string) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaGetDebugStringReply(fbb, fbb.CreateString(debug_string));
  return PlasmaSend(client, MessageType::PlasmaGetDebugStringReply, &fbb, message);
}

Status ReadGetDebugStringReply(uint8_t *data, size_t size, std::string *debug_string) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetDebugStringReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *debug_string = message->debug_string()->str();
  return Status::OK();
}

// Create messages.

Status SendCreateRetryRequest(const std::shared_ptr<StoreConn> &store_conn,
                              ObjectID object_id,
                              uint64_t request_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaCreateRetryRequest(
      fbb, fbb.CreateString(object_id.Binary()), request_id);
  return PlasmaSend(store_conn, MessageType::PlasmaCreateRetryRequest, &fbb, message);
}

Status SendCreateRequest(const std::shared_ptr<StoreConn> &store_conn,
                         ObjectID object_id,
                         const ray::rpc::Address &owner_address,
                         int64_t data_size,
                         int64_t metadata_size,
                         flatbuf::ObjectSource source,
                         int device_num,
                         bool try_immediately) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaCreateRequest(fbb,
                                    fbb.CreateString(object_id.Binary()),
                                    fbb.CreateString(owner_address.raylet_id()),
                                    fbb.CreateString(owner_address.ip_address()),
                                    owner_address.port(),
                                    fbb.CreateString(owner_address.worker_id()),
                                    data_size,
                                    metadata_size,
                                    source,
                                    device_num,
                                    try_immediately);
  return PlasmaSend(store_conn, MessageType::PlasmaCreateRequest, &fbb, message);
}

void ReadCreateRequest(uint8_t *data,
                       size_t size,
                       ray::ObjectInfo *object_info,
                       flatbuf::ObjectSource *source,
                       int *device_num) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaCreateRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  object_info->data_size = message->data_size();
  object_info->metadata_size = message->metadata_size();
  object_info->object_id = ObjectID::FromBinary(message->object_id()->str());
  object_info->owner_raylet_id = NodeID::FromBinary(message->owner_raylet_id()->str());
  object_info->owner_ip_address = message->owner_ip_address()->str();
  object_info->owner_port = message->owner_port();
  object_info->owner_worker_id = WorkerID::FromBinary(message->owner_worker_id()->str());
  *source = message->source();
  *device_num = message->device_num();
  return;
}

Status SendUnfinishedCreateReply(const std::shared_ptr<Client> &client,
                                 ObjectID object_id,
                                 uint64_t retry_with_request_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto object_string = fbb.CreateString(object_id.Binary());
  fb::PlasmaCreateReplyBuilder crb(fbb);
  crb.add_object_id(object_string);
  crb.add_retry_with_request_id(retry_with_request_id);
  auto message = crb.Finish();
  return PlasmaSend(client, MessageType::PlasmaCreateReply, &fbb, message);
}

Status SendCreateReply(const std::shared_ptr<Client> &client,
                       ObjectID object_id,
                       const PlasmaObject &object,
                       PlasmaError error_code) {
  flatbuffers::FlatBufferBuilder fbb;
  PlasmaObjectSpec plasma_object(FD2INT(object.store_fd.first),
                                 object.store_fd.second,
                                 object.data_offset,
                                 object.data_size,
                                 object.metadata_offset,
                                 object.metadata_size,
                                 object.device_num);
  auto object_string = fbb.CreateString(object_id.Binary());
  fb::PlasmaCreateReplyBuilder crb(fbb);
  crb.add_error(static_cast<PlasmaError>(error_code));
  crb.add_plasma_object(&plasma_object);
  crb.add_object_id(object_string);
  crb.add_retry_with_request_id(0);
  crb.add_store_fd(FD2INT(object.store_fd.first));
  crb.add_unique_fd_id(object.store_fd.second);
  crb.add_mmap_size(object.mmap_size);
  if (object.device_num != 0) {
    RAY_LOG(FATAL) << "This should be unreachable.";
  }
  auto message = crb.Finish();
  return PlasmaSend(client, MessageType::PlasmaCreateReply, &fbb, message);
}

Status ReadCreateReply(uint8_t *data,
                       size_t size,
                       ObjectID *object_id,
                       uint64_t *retry_with_request_id,
                       PlasmaObject *object,
                       MEMFD_TYPE *store_fd,
                       int64_t *mmap_size) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaCreateReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  *retry_with_request_id = message->retry_with_request_id();
  if (*retry_with_request_id > 0) {
    // The client should retry the request.
    return Status::OK();
  }

  object->store_fd.first = INT2FD(message->plasma_object()->segment_index());
  object->store_fd.second = message->plasma_object()->unique_fd_id();
  object->data_offset = message->plasma_object()->data_offset();
  object->data_size = message->plasma_object()->data_size();
  object->metadata_offset = message->plasma_object()->metadata_offset();
  object->metadata_size = message->plasma_object()->metadata_size();

  store_fd->first = INT2FD(message->store_fd());
  store_fd->second = message->unique_fd_id();
  *mmap_size = message->mmap_size();

  object->device_num = message->plasma_object()->device_num();
  return PlasmaErrorStatus(message->error());
}

Status SendAbortRequest(const std::shared_ptr<StoreConn> &store_conn,
                        ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaAbortRequest(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(store_conn, MessageType::PlasmaAbortRequest, &fbb, message);
}

Status ReadAbortRequest(uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaAbortRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return Status::OK();
}

Status SendAbortReply(const std::shared_ptr<Client> &client, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaAbortReply(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(client, MessageType::PlasmaAbortReply, &fbb, message);
}

Status ReadAbortReply(uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaAbortReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return Status::OK();
}

// Seal messages.

Status SendSealRequest(const std::shared_ptr<StoreConn> &store_conn, ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaSealRequest(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(store_conn, MessageType::PlasmaSealRequest, &fbb, message);
}

Status ReadSealRequest(uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaSealRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return Status::OK();
}

Status SendSealReply(const std::shared_ptr<Client> &client,
                     ObjectID object_id,
                     PlasmaError error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaSealReply(fbb, fbb.CreateString(object_id.Binary()), error);
  return PlasmaSend(client, MessageType::PlasmaSealReply, &fbb, message);
}

Status ReadSealReply(uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaSealReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return PlasmaErrorStatus(message->error());
}

// Release messages.

Status SendReleaseRequest(const std::shared_ptr<StoreConn> &store_conn,
                          ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaReleaseRequest(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(store_conn, MessageType::PlasmaReleaseRequest, &fbb, message);
}

Status ReadReleaseRequest(uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaReleaseRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return Status::OK();
}

Status SendReleaseReply(const std::shared_ptr<Client> &client,
                        ObjectID object_id,
                        PlasmaError error) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaReleaseReply(fbb, fbb.CreateString(object_id.Binary()), error);
  return PlasmaSend(client, MessageType::PlasmaReleaseReply, &fbb, message);
}

Status ReadReleaseReply(uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaReleaseReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return PlasmaErrorStatus(message->error());
}

// Delete objects messages.

Status SendDeleteRequest(const std::shared_ptr<StoreConn> &store_conn,
                         const std::vector<ObjectID> &object_ids) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaDeleteRequest(
      fbb,
      static_cast<int32_t>(object_ids.size()),
      ToFlatbuffer(&fbb, &object_ids[0], object_ids.size()));
  return PlasmaSend(store_conn, MessageType::PlasmaDeleteRequest, &fbb, message);
}

Status ReadDeleteRequest(uint8_t *data, size_t size, std::vector<ObjectID> *object_ids) {
  using fb::PlasmaDeleteRequest;

  RAY_DCHECK(data);
  RAY_DCHECK(object_ids);
  auto message = flatbuffers::GetRoot<PlasmaDeleteRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  ToVector(*message, object_ids, [](const PlasmaDeleteRequest &request, int i) {
    return ObjectID::FromBinary(request.object_ids()->Get(i)->str());
  });
  return Status::OK();
}

Status SendDeleteReply(const std::shared_ptr<Client> &client,
                       const std::vector<ObjectID> &object_ids,
                       const std::vector<PlasmaError> &errors) {
  RAY_DCHECK(object_ids.size() == errors.size());
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaDeleteReply(
      fbb,
      static_cast<int32_t>(object_ids.size()),
      ToFlatbuffer(&fbb, &object_ids[0], object_ids.size()),
      fbb.CreateVector(MakeNonNull(reinterpret_cast<const int32_t *>(errors.data())),
                       object_ids.size()));
  return PlasmaSend(client, MessageType::PlasmaDeleteReply, &fbb, message);
}

Status ReadDeleteReply(uint8_t *data,
                       size_t size,
                       std::vector<ObjectID> *object_ids,
                       std::vector<PlasmaError> *errors) {
  using fb::PlasmaDeleteReply;

  RAY_DCHECK(data);
  RAY_DCHECK(object_ids);
  RAY_DCHECK(errors);
  auto message = flatbuffers::GetRoot<PlasmaDeleteReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  ToVector(*message, object_ids, [](const PlasmaDeleteReply &request, int i) {
    return ObjectID::FromBinary(request.object_ids()->Get(i)->str());
  });
  ToVector(*message, errors, [](const PlasmaDeleteReply &request, int i) {
    return static_cast<PlasmaError>(request.errors()->data()[i]);
  });
  return Status::OK();
}

// Contains messages.

Status SendContainsRequest(const std::shared_ptr<StoreConn> &store_conn,
                           ObjectID object_id) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaContainsRequest(fbb, fbb.CreateString(object_id.Binary()));
  return PlasmaSend(store_conn, MessageType::PlasmaContainsRequest, &fbb, message);
}

Status ReadContainsRequest(uint8_t *data, size_t size, ObjectID *object_id) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaContainsRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  return Status::OK();
}

Status SendContainsReply(const std::shared_ptr<Client> &client,
                         ObjectID object_id,
                         bool has_object) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaContainsReply(
      fbb, fbb.CreateString(object_id.Binary()), has_object);
  return PlasmaSend(client, MessageType::PlasmaContainsReply, &fbb, message);
}

Status ReadContainsReply(uint8_t *data,
                         size_t size,
                         ObjectID *object_id,
                         bool *has_object) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaContainsReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  *has_object = message->has_object();
  return Status::OK();
}

// Connect messages.

Status SendConnectRequest(const std::shared_ptr<StoreConn> &store_conn) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaConnectRequest(fbb);
  return PlasmaSend(store_conn, MessageType::PlasmaConnectRequest, &fbb, message);
}

Status ReadConnectRequest(uint8_t *data) { return Status::OK(); }

Status SendConnectReply(const std::shared_ptr<Client> &client, int64_t memory_capacity) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaConnectReply(fbb, memory_capacity);
  return PlasmaSend(client, MessageType::PlasmaConnectReply, &fbb, message);
}

Status ReadConnectReply(uint8_t *data, size_t size, int64_t *memory_capacity) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaConnectReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *memory_capacity = message->memory_capacity();
  return Status::OK();
}

// Evict messages.

Status SendEvictRequest(const std::shared_ptr<StoreConn> &store_conn, int64_t num_bytes) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaEvictRequest(fbb, num_bytes);
  return PlasmaSend(store_conn, MessageType::PlasmaEvictRequest, &fbb, message);
}

Status ReadEvictRequest(uint8_t *data, size_t size, int64_t *num_bytes) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaEvictRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  *num_bytes = message->num_bytes();
  return Status::OK();
}

Status SendEvictReply(const std::shared_ptr<Client> &client, int64_t num_bytes) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaEvictReply(fbb, num_bytes);
  return PlasmaSend(client, MessageType::PlasmaEvictReply, &fbb, message);
}

Status ReadEvictReply(uint8_t *data, size_t size, int64_t &num_bytes) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaEvictReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  num_bytes = message->num_bytes();
  return Status::OK();
}

// Get messages.

Status SendGetRequest(const std::shared_ptr<StoreConn> &store_conn,
                      const ObjectID *object_ids,
                      int64_t num_objects,
                      int64_t timeout_ms,
                      bool is_from_worker) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message = fb::CreatePlasmaGetRequest(
      fbb, ToFlatbuffer(&fbb, object_ids, num_objects), timeout_ms, is_from_worker);
  return PlasmaSend(store_conn, MessageType::PlasmaGetRequest, &fbb, message);
}

Status SendGetRemoteRequest(const std::shared_ptr<StoreConn> &store_conn,
                            const std::string &ip_addr,
                            const ObjectID &object_id,
                            int64_t pinned_at_off,
                            bool is_from_worker) {
  flatbuffers::FlatBufferBuilder fbb;
  auto message =
      fb::CreatePlasmaGetRemoteRequest(fbb,
                                       fbb.CreateString(ip_addr),
                                       fbb.CreateString(object_id.Binary()),
                                       pinned_at_off,
                                       is_from_worker);
  return PlasmaSend(store_conn, MessageType::PlasmaGetRemoteRequest, &fbb, message);
}

Status ReadGetRequest(uint8_t *data,
                      size_t size,
                      std::vector<ObjectID> &object_ids,
                      int64_t *timeout_ms,
                      bool *is_from_worker) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetRequest>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  for (uoffset_t i = 0; i < message->object_ids()->size(); ++i) {
    auto object_id = message->object_ids()->Get(i)->str();
    object_ids.push_back(ObjectID::FromBinary(object_id));
  }
  *timeout_ms = message->timeout_ms();
  *is_from_worker = message->is_from_worker();
  return Status::OK();
}

Status ReadGetRemoteRequest(uint8_t *req_buf,
                            size_t req_size,
                            std::string *owner_ip_address,
                            ObjectID *object_id,
                            int64_t *pinned_at_off,
                            bool *is_from_worker) {
  RAY_DCHECK(req_buf);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetRemoteRequest>(req_buf);
  RAY_CHECK(VerifyFlatbuffer(message, req_buf, req_size));
  *owner_ip_address = message->owner_ip_address()->str();
  *object_id = ObjectID::FromBinary(message->object_id()->str());
  *pinned_at_off = message->pinned_at_off();
  *is_from_worker = message->is_from_worker();

  return Status::OK();
}

Status SendGetReply(const std::shared_ptr<Client> &client,
                    ObjectID object_ids[],
                    absl::flat_hash_map<ObjectID, PlasmaObject> &plasma_objects,
                    int64_t num_objects,
                    const std::vector<MEMFD_TYPE> &store_fds,
                    const std::vector<int64_t> &mmap_sizes) {
  flatbuffers::FlatBufferBuilder fbb;
  std::vector<PlasmaObjectSpec> objects;

  std::vector<flatbuffers::Offset<fb::CudaHandle>> handles;
  for (int64_t i = 0; i < num_objects; ++i) {
    const PlasmaObject &object = plasma_objects[object_ids[i]];
    RAY_LOG(DEBUG) << "Sending object info, id: " << object_ids[i]
                   << " data_size: " << object.data_size
                   << " metadata_size: " << object.metadata_size;
    objects.push_back(PlasmaObjectSpec(FD2INT(object.store_fd.first),
                                       object.store_fd.second,
                                       object.data_offset,
                                       object.data_size,
                                       object.metadata_offset,
                                       object.metadata_size,
                                       object.device_num));
  }
  std::vector<int> store_fds_as_int;
  std::vector<int64_t> unique_fd_ids;
  for (MEMFD_TYPE store_fd : store_fds) {
    store_fds_as_int.push_back(FD2INT(store_fd.first));
    unique_fd_ids.push_back(store_fd.second);
  }
  auto message = fb::CreatePlasmaGetReply(
      fbb,
      ToFlatbuffer(&fbb, object_ids, num_objects),
      fbb.CreateVectorOfStructs(MakeNonNull(objects.data()), num_objects),
      fbb.CreateVector(MakeNonNull(store_fds_as_int.data()), store_fds_as_int.size()),
      fbb.CreateVector(MakeNonNull(unique_fd_ids.data()), unique_fd_ids.size()),
      fbb.CreateVector(MakeNonNull(mmap_sizes.data()), mmap_sizes.size()),
      fbb.CreateVector(MakeNonNull(handles.data()), handles.size()));
  return PlasmaSend(client, MessageType::PlasmaGetReply, &fbb, message);
}

Status ReadGetReply(uint8_t *data,
                    size_t size,
                    ObjectID object_ids[],
                    PlasmaObject plasma_objects[],
                    int64_t num_objects,
                    std::vector<MEMFD_TYPE> &store_fds,
                    std::vector<int64_t> &mmap_sizes) {
  RAY_DCHECK(data);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetReply>(data);
  RAY_DCHECK(VerifyFlatbuffer(message, data, size));
  for (uoffset_t i = 0; i < num_objects; ++i) {
    object_ids[i] = ObjectID::FromBinary(message->object_ids()->Get(i)->str());
  }
  for (uoffset_t i = 0; i < num_objects; ++i) {
    const PlasmaObjectSpec *object = message->plasma_objects()->Get(i);
    plasma_objects[i].store_fd.first = INT2FD(object->segment_index());
    plasma_objects[i].store_fd.second = object->unique_fd_id();
    plasma_objects[i].data_offset = object->data_offset();
    plasma_objects[i].data_size = object->data_size();
    plasma_objects[i].metadata_offset = object->metadata_offset();
    plasma_objects[i].metadata_size = object->metadata_size();
    plasma_objects[i].device_num = object->device_num();
  }
  RAY_CHECK(message->store_fds()->size() == message->mmap_sizes()->size());
  for (uoffset_t i = 0; i < message->store_fds()->size(); i++) {
    store_fds.push_back(
        {INT2FD(message->store_fds()->Get(i)), message->unique_fd_ids()->Get(i)});
    mmap_sizes.push_back(message->mmap_sizes()->Get(i));
  }
  return Status::OK();
}

Status ReadGetRemoteReply(uint8_t *reply_buf,
                          size_t reply_buf_size,
                          ObjectID &object_id,
                          PlasmaObject &plasma_object,
                          MEMFD_TYPE &store_fd,
                          int64_t &mmap_size) {
  RAY_DCHECK(reply_buf);
  auto message = flatbuffers::GetRoot<fb::PlasmaGetRemoteReply>(reply_buf);
  RAY_DCHECK(VerifyFlatbuffer(message, reply_buf, reply_buf_size));
  object_id = ObjectID::FromBinary(message->object_id()->str());
  const PlasmaObjectSpec *object = message->plasma_object();
  plasma_object.store_fd.first = INT2FD(object->segment_index());
  plasma_object.store_fd.second = object->unique_fd_id();
  plasma_object.data_offset = object->data_offset();
  plasma_object.data_size = object->data_size();
  plasma_object.metadata_offset = object->metadata_offset();
  plasma_object.metadata_size = object->metadata_size();
  plasma_object.device_num = object->device_num();
  store_fd = {INT2FD(message->store_fd()), message->unique_fd_id()};
  mmap_size = message->mmap_size();

  return Status::OK();
}

}  // namespace plasma
