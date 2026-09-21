from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libc.stdint cimport int64_t

from ray.includes.common cimport CAddress, CRayStatus

cdef extern from "src/ray/protobuf/common.pb.h" namespace "ray::rpc" nogil:
    cdef cppclass CRecoveryStreamDescriptor "ray::rpc::RecoveryStreamDescriptor":
        CRecoveryStreamDescriptor()
        c_bool ParseFromString(const c_string &data)
        const c_string &task_id() const
        const c_string &generator_id() const
        int64_t expected_returns() const
        const CAddress &consumer_address() const

cdef extern from "ray/common/streaming_recovery/streaming_recovery.h" namespace "ray" nogil:
    int64_t RecoveryStreamReturnLimit(const CRecoveryStreamDescriptor &descriptor)
    CRayStatus ValidateRecoveryStreamDescriptor(
        const CRecoveryStreamDescriptor &descriptor)
