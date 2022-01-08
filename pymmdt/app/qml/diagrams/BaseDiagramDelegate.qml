import QtQuick 2.0

Component {

    // Entire dtype slot
    Rectangle {
        id: dtype_slot
        anchors.left: parent.left
        anchors.right: parent.right
        height: 200
        color: "green"

        // Title of dtype
        Text {
            height: 50
            anchors.top: parent.top
            anchors.left: parent.left
            anchors.right: parent.right
            text: qsTr(dtype)
            horizontalAlignment: Text.AlignLeft
        }

        Row {
            anchors.verticalCenter: parent.verticalCenter
            spacing: 30

            Repeater {
                model: modality
                Rectangle {
                    width: height * (16/9)
                    height: dtype_slot.height - 40
                    color: "blue"

                    Loader {
                        source: switch(dtype) {
                            case "video": return Qt.resolvedUrl("VideoDelegate.qml")
                            case "image": return Qt.resolvedUrl("ImageDelegate.qml")
                         }
                     }
                }
            }
        }
    }
}
