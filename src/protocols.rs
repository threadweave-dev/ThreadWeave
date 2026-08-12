pub mod common {
    pub mod v1 {
        tonic::include_proto!("threadweave_protocols.common.v1");
    }
}
pub mod artifacts {
    pub mod v1 {
        tonic::include_proto!("threadweave_protocols.artifacts.v1");
    }
}
pub mod execution {
    pub mod v1 {
        tonic::include_proto!("threadweave_protocols.execution.v1");
    }
}
pub mod runtime {
    pub mod v1 {
        tonic::include_proto!("threadweave_protocols.runtime.v1");
    }
}
pub mod broker {
    pub mod v1 {
        tonic::include_proto!("threadweave_protocols.broker.v1");
    }
}
