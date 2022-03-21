
import QtQuick 2.0

Component {

    Rectangle {
        id: totalUserRect
        anchors.left: parent.left
        anchors.right: parent.right
        height: timelinesColumn.childrenRect.height
        color: "#282a36"

        Rectangle {
            id: userTitleRect
            anchors.left: parent.left
            width: 30
            height: parent.height
            // color: "white"
            color: "#282a36"

            Text {
                height: totalUserRect.height
                color: "white"
                // color: "#282a36"
                anchors.top: parent.top
                anchors.left: parent.left
                anchors.right: parent.right
                text: qsTr(user)
                horizontalAlignment: Text.AlignLeft
            }
        }

        Rectangle {
            id: userTimelines
            anchors.left: userTitleRect.right
            anchors.right: parent.right
            height: childrenRect.height
            width: totalUserRect.width - userTitleRect.width
            // color: "orange"

            Column {
                id: timelinesColumn
                height: parent.height
                width: parent.width
                spacing: 0

                Repeater {
                    id: timelineView
                    model: timelines
                    Rectangle {
                        height: 30
                        anchors.left: parent.left
                        anchors.right: parent.right
                        color: "#282a36"

                        Loader {
                            source: switch(dtype) {
                                case "video": return Qt.resolvedUrl("VideoTimelineDelegate.qml")
                                case "image": return Qt.resolvedUrl("ImageTimelineDelegate.qml")
                             }
                        }
                    }
                }
            }
        }
    }
}
