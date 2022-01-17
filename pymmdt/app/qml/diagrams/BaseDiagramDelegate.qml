import QtQuick 2.0
import QtQuick.Window 2.1

Component {

    Rectangle {
        id: groupRectangle
        anchors.left: parent.left
        anchors.right: parent.right
        height: entries.childrenRect.height + entryTitle.height
        color: "green"

        // Title of dtype
        Text {
            id: entryTitle
            height: 50
            anchors.top: parent.top
            anchors.left: parent.left
            anchors.right: parent.right
            text: qsTr(sort_by)
            horizontalAlignment: Text.AlignLeft
        }

        // Repeater {
        Flow {
            id: entries
            anchors.fill: parent
            anchors.margins: 15
            anchors.verticalCenter: parent.verticalCenter
            spacing: 15

            Repeater {
                model: group
                Rectangle {
                    id: groupRectangle
                    width: 0.2*Screen.width
                    height: width * (9/16)
                    color: "blue"

                    Loader {
                        source: switch(dtype) {
                            case "video": return Qt.resolvedUrl("VideoDelegate.qml")
                            case "image": return Qt.resolvedUrl("ImageDelegate.qml")
                        }
                        property int _height: groupRectangle.height
                        property int _width: groupRectangle.width
                     }
                }
            }
        }
    }
}
